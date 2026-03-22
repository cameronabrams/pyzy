"""
Assignment scorer - processes zyBooks per-assignment report CSVs, applies
late penalties from a due-dates table, and updates BBLearn lecture gradebooks.
"""

import csv
import math
import re
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from .common import (
    extract_username_from_email,
    find_email_column,
    find_student_id_column,
    fmt_late,
    load_weights_csv,
    middle_name_matched as _middle_name_matched,
    normalize_student_id,
    read_csv_with_trailing_comma_fix,
    recompute_averages,
    resolve_column,
)


def parse_assignment_filename_short(filename):
    """Parse assignment name from zyBooks filename, returning abbreviated form.
    e.g. 'Week_3_Participation_Activities_report.csv' -> 'W3 PA'
    """
    stem = Path(filename).stem

    match = re.search(
        r'Week[_\s]+(\d+)[_\s]+(Participation|Challenge|In-Lab|Out-of-Lab)[_\s]+(Activities|Labs)',
        stem, re.IGNORECASE,
    )

    if match:
        week_num = match.group(1)
        assignment_type = match.group(2).strip()
        assignment_kind = match.group(3).strip()

        type_map = {
            'Participation Activities': 'PA',
            'Challenge Activities': 'CA',
            'In-Lab Labs': 'IL',
            'Out-of-Lab Labs': 'OL',
        }

        abbrev_type = type_map.get(f"{assignment_type} {assignment_kind}")
        if abbrev_type:
            return f"W{week_num} {abbrev_type}"

    return stem


_KNOWN_TYPES = {'PA', 'CA', 'OL', 'IL'}


def assignment_name_from_path(filepath):
    """
    Derive an assignment name (e.g. 'W3 PA') from a file path.

    Two conventions are supported:
    1. Full zyBooks filename: 'Week_3_Participation_Activities_report.csv'
    2. Directory-based: 'PA/W3.csv'  (parent dir = type, stem = week)
    """
    p = Path(filepath)
    result = parse_assignment_filename_short(p.name)
    if result != p.stem:
        return result  # full filename parse succeeded

    # Try directory-based convention
    dir_type = p.parent.name.upper()
    if dir_type in _KNOWN_TYPES:
        m = re.match(r'^W?(\d+)$', p.stem, re.IGNORECASE)
        if m:
            return f"W{m.group(1)} {dir_type}"

    return result  # fallback: bare stem


_LOCAL_TZ = ZoneInfo('America/New_York')


def _parse_due_date_str(s):
    """
    Parse a date/time string; treats naive datetimes as America/New_York.
    Handles '24:00' by converting to 00:00 of the following day.
    """
    s = s.strip()
    if '24:00' in s:
        s = s.replace('24:00', '00:00')
        dt = pd.to_datetime(s) + pd.Timedelta(days=1)
    else:
        dt = pd.to_datetime(s)
    if dt.tzinfo is None:
        dt = dt.tz_localize(_LOCAL_TZ)
    return dt.tz_convert('UTC')


def load_due_dates_csv(path):
    """
    Load due dates from a CSV with one row per week.

    Expected format:
        Week column (any name containing 'week', or the first column)
        Type columns: PA, CA, OL, IL60, IL61, ... (one per assignment type/section)

    Returns:
        dict mapping (week_int, type_str) -> UTC-aware Timestamp
    """
    df = pd.read_csv(path)
    week_col = next((c for c in df.columns if 'week' in c.lower()), df.columns[0])
    result = {}
    for _, row in df.iterrows():
        m = re.search(r'\d+', str(row[week_col]))
        if not m:
            continue
        week_num = int(m.group())
        for col in df.columns:
            if col == week_col:
                continue
            val = row[col]
            if pd.isna(val) or str(val).strip() == '':
                continue
            try:
                result[(week_num, col.strip())] = _parse_due_date_str(str(val).strip())
            except Exception:
                pass
    return result


def _late_penalty_factor(delta_seconds, days_grace, penalty, hours_grace=0, grace_limit=None):
    """
    Flat deduction: no penalty within grace period, then subtract `penalty`
    fraction once for any lateness beyond it.
    If grace_limit is set (days), submissions more than that many days late are zeroed.
    """
    if delta_seconds <= days_grace * 86400 + hours_grace * 3600:
        return 1.0
    if grace_limit is not None and delta_seconds > grace_limit * 86400:
        return 0.0
    return max(0.0, 1.0 - penalty)


def _build_lab_section_map(lecture_dfs):
    """
    Build student → lab section lookup maps from loaded lecture gradebooks.

    Returns:
        (username_map, id_map) where values are section strings like '60', '61'
    """
    from .common import resolve_column
    from .merge import find_username_column

    username_map = {}
    id_map = {}

    for df in lecture_dfs.values():
        try:
            lab_col = resolve_column(df, 'Lab section')
        except ValueError:
            continue

        un_col = find_username_column(df)
        em_col = find_email_column(df)
        id_col = find_student_id_column(df)

        for _, row in df.iterrows():
            section = str(row[lab_col]).strip() if pd.notna(row[lab_col]) else ''
            if not section or section.lower() == 'nan':
                continue

            username = None
            if un_col and not pd.isna(row.get(un_col)):
                username = str(row[un_col]).strip().lower()
            elif em_col:
                username = extract_username_from_email(row[em_col])
            if username:
                username_map[username] = section

            if id_col:
                sid = normalize_student_id(row[id_col])
                if sid:
                    id_map[sid] = section

    return username_map, id_map


def _apply_late_penalties(df, due_dt, days_grace, penalty, assignment_name, verbose=True,
                          lab_section_map=None, id_section_map=None, section_due_dates=None,
                          date_audit=False, hours_grace=0, grace_limit=None, no_penalty_ids=None):
    """
    Apply late penalties in-place using the 'Score date' column.

    For non-IL assignments: due_dt is a single UTC Timestamp.
    For IL assignments: due_dt=None; per-student due date is resolved from
    lab_section_map / id_section_map + section_due_dates.

    Returns:
        (late_records, audit_records)
        late_records  — list of dicts for students who received a penalty
        audit_records — list of dicts for ALL students with score dates
                        (only populated when date_audit=True)
    """
    score_date_col = None
    score_col = None
    for col in df.columns:
        cl = col.lower()
        if 'score date' in cl:
            score_date_col = col
        elif 'percent score' in cl:
            score_col = col

    if date_audit:
        print(f"   [date-audit] columns: {list(df.columns)}")
        print(f"   [date-audit] score_date_col={score_date_col!r}  score_col={score_col!r}")
        if score_date_col:
            non_null = df[score_date_col].dropna()
            print(f"   [date-audit] score date non-null count: {len(non_null)}/{len(df)}")
            if len(non_null):
                print(f"   [date-audit] sample score dates: {list(non_null.head(3))}")

    if not score_date_col or not score_col:
        if verbose:
            print(f"   WARNING: Missing 'Score date' or 'Percent score' column "
                  f"— skipping late penalty for {assignment_name}")
        return [], []

    email_col = find_email_column(df)
    id_col = find_student_id_column(df)
    il_mode = due_dt is None

    if verbose:
        if not il_mode:
            local_due = due_dt.tz_convert(_LOCAL_TZ)
            print(f"   Due: {local_due.strftime('%Y-%m-%d %H:%M %Z')}  |  "
                  f"Grace: {days_grace}d {hours_grace}h  |  Penalty: {penalty*100:.0f}%")
            valid_dates = df[score_date_col].dropna()
            if len(valid_dates):
                try:
                    parsed = pd.to_datetime(valid_dates, utc=True)
                    print(f"   Score dates: {len(valid_dates)} student(s)  |  "
                          f"earliest: {parsed.min().tz_convert(_LOCAL_TZ).strftime('%Y-%m-%d %H:%M')}  |  "
                          f"latest:   {parsed.max().tz_convert(_LOCAL_TZ).strftime('%Y-%m-%d %H:%M')}")
                except Exception:
                    print(f"   Score dates: {len(valid_dates)} student(s)")
        else:
            print(f"   IL sections: Grace: {days_grace}d {hours_grace}h  |  Penalty: {penalty*100:.0f}%")
            for sec in sorted(section_due_dates or {}):
                dt = section_due_dates[sec].tz_convert(_LOCAL_TZ)
                print(f"      IL{sec}: due {dt.strftime('%Y-%m-%d %H:%M %Z')}")

    df[score_col] = df[score_col].astype(object)

    late_records = []
    audit_records = []
    n_late = 0
    n_penalized = 0
    n_no_section = 0

    for idx, row in df.iterrows():
        first_name = ''
        last_name = ''
        for col in row.index:
            cl = col.lower()
            if 'first' in cl and 'name' in cl:
                first_name = str(row[col]) if pd.notna(row[col]) else ''
            elif 'last' in cl and 'name' in cl:
                last_name = str(row[col]) if pd.notna(row[col]) else ''

        student_sid = normalize_student_id(row[id_col]) if id_col else None
        score_date_str = str(row[score_date_col]).strip() if pd.notna(row[score_date_col]) else ''
        if not score_date_str:
            audit_records.append({
                'Last Name': last_name,
                'First Name': first_name,
                'Student ID': student_sid or '',
                'Lab Section': '',
                'School Email': row[email_col] if email_col else '',
                'Score Date (local)': '',
                'Due Date (local)': '',
                'Delta': '',
                'Status': 'no submission',
                'Penalty Factor': '',
                'Original Score': None,
                'Applied Score': None,
            })
            continue

        try:
            # zyBooks format: "2026-01-14 10:35 PM EST" / "... EDT"
            # Strip the abbreviation and localize; use it to resolve DST ambiguity.
            tz_match = re.search(r'\b([A-Z]{2,4})$', score_date_str.strip())
            is_dst = tz_match.group(1).endswith('DT') if tz_match else False
            clean = re.sub(r'\s+[A-Z]{2,4}$', '', score_date_str.strip())
            sub_dt = pd.to_datetime(clean)
            sub_dt = sub_dt.tz_localize(_LOCAL_TZ, ambiguous=is_dst, nonexistent='shift_forward')
            sub_dt = sub_dt.tz_convert('UTC')
        except Exception:
            audit_records.append({
                'Last Name': last_name,
                'First Name': first_name,
                'Student ID': student_sid or '',
                'Lab Section': '',
                'School Email': row[email_col] if email_col else '',
                'Score Date (local)': score_date_str,
                'Due Date (local)': '',
                'Delta': '',
                'Status': 'parse error',
                'Penalty Factor': '',
                'Original Score': None,
                'Applied Score': None,
            })
            continue

        sub_dt_local = sub_dt.tz_convert(_LOCAL_TZ)

        # Resolve effective due date
        if il_mode:
            username = extract_username_from_email(row[email_col]) if email_col else None
            section = (
                (lab_section_map or {}).get(username) or
                (id_section_map or {}).get(student_sid)
            )
            if not section:
                n_no_section += 1
                audit_records.append({
                    'Last Name': last_name,
                    'First Name': first_name,
                    'Student ID': student_sid or '',
                    'Lab Section': section or '',
                    'School Email': row[email_col] if email_col else '',
                    'Score Date (local)': sub_dt_local.isoformat(timespec='minutes'),
                    'Due Date (local)': '',
                    'Delta': '',
                    'Status': 'no section',
                    'Penalty Factor': '',
                    'Original Score': None,
                    'Applied Score': None,
                })
                continue
            effective_due_dt = (section_due_dates or {}).get(section)
            if effective_due_dt is None:
                continue
        else:
            effective_due_dt = due_dt
            username = extract_username_from_email(row[email_col]) if email_col else None
            section = (
                (lab_section_map or {}).get(username) or
                (id_section_map or {}).get(student_sid)
            ) or None

        due_dt_local = effective_due_dt.tz_convert(_LOCAL_TZ)
        delta = (sub_dt - effective_due_dt).total_seconds()
        original_score = row[score_col]

        exempt = no_penalty_ids and student_sid and student_sid in no_penalty_ids

        if delta > 0:
            n_late += 1
            if exempt:
                factor = 1.0
                penalized_score = original_score
                status = 'exempt'
            elif not pd.isna(original_score):
                factor = _late_penalty_factor(delta, days_grace, penalty, hours_grace, grace_limit)
                penalized_score = original_score * factor
                df.at[idx, score_col] = penalized_score
                if factor < 1.0:
                    n_penalized += 1
                within_grace = delta <= days_grace * 86400 + hours_grace * 3600
                status = 'grace' if within_grace else 'late'
            else:
                factor = 1.0
                penalized_score = original_score
                within_grace = delta <= days_grace * 86400 + hours_grace * 3600
                status = 'grace' if within_grace else 'late'

            rec = {
                'Last Name': last_name,
                'First Name': first_name,
                'Student ID': student_sid or '',
                'Lab Section': section or '',
                'School Email': row[email_col] if email_col else '',
                'Score Date': score_date_str,
                'How Late': fmt_late(delta),
                'Original Score': round(float(original_score), 4) if not pd.isna(original_score) else '',
                'Penalty Factor': round(factor, 4),
                'Applied Score': round(penalized_score, 4) if not pd.isna(penalized_score) else '',
            }
            late_records.append(rec)
        else:
            factor = 1.0
            status = 'on time'

        penalized_for_rec = penalized_score if delta > 0 else original_score
        audit_rec = {
            'Last Name': last_name,
            'First Name': first_name,
            'Student ID': normalize_student_id(row[id_col]) if id_col else '',
            'School Email': row[email_col] if email_col else '',
            'Score Date (local)': sub_dt_local.strftime('%Y-%m-%d %H:%M'),
            'Due Date (local)': due_dt_local.strftime('%Y-%m-%d %H:%M'),
            'Delta': fmt_late(delta) if delta > 0 else f"-{fmt_late(-delta)}",
            'Status': status,
            'Penalty Factor': round(factor, 4) if status == 'late' else '',
            'Original Score': round(float(original_score), 4) if not pd.isna(original_score) else None,
            'Applied Score': round(float(penalized_for_rec), 4) if not pd.isna(penalized_for_rec) else None,
        }
        audit_rec['Lab Section'] = section or ''
        audit_records.append(audit_rec)

    if verbose:
        print(f"   Late: {n_late} student(s), penalized: {n_penalized}")
        if il_mode and n_no_section:
            print(f"   Skipped (no section assigned): {n_no_section} student(s)")

    return late_records, audit_records


def run_revert(pattern, lecture_files, original_files, quiet=False, weights_csv=None):
    """
    Copy a single assignment column from original gradebooks back into working copies.

    Matches pairs by filename.  Uses resolve_column(df, pattern) to locate the
    column in each gradebook.  Writes working copies in-place with quoting=csv.QUOTE_ALL.

    Args:
        pattern:        Substring identifying the target column (e.g. 'W3 PA')
        lecture_files:  Working BBLearn gradebook CSV paths
        original_files: Original (unmodified) BBLearn gradebook CSV paths
        quiet:          Suppress verbose output
    """
    verbose = not quiet

    # Build filename -> path map for originals
    orig_by_name = {}
    for of in original_files:
        op = Path(of)
        if not op.exists():
            print(f"ERROR: Original not found: {of}")
            sys.exit(1)
        orig_by_name[op.name] = op

    print("\nRevert Assignment Column")
    print("=" * 60)
    print(f"Pattern: '{pattern}'")

    reverted = 0

    for lf in lecture_files:
        lp = Path(lf)
        if not lp.exists():
            print(f"ERROR: Working gradebook not found: {lf}")
            continue

        if lp.name not in orig_by_name:
            print(f"WARNING: No matching original for '{lp.name}' — skipping")
            continue

        work_df = read_csv_with_trailing_comma_fix(lp)
        orig_df = read_csv_with_trailing_comma_fix(orig_by_name[lp.name])

        try:
            work_col = resolve_column(work_df, pattern)
        except ValueError as e:
            print(f"ERROR in working '{lp.name}': {e}")
            continue

        try:
            orig_col = resolve_column(orig_df, pattern)
        except ValueError as e:
            print(f"ERROR in original '{lp.name}': {e}")
            continue

        if verbose:
            print(f"\n   {lp.name}")
            print(f"      Working column : '{work_col}'")
            print(f"      Original column: '{orig_col}'")

        work_id_col = find_student_id_column(work_df)
        orig_id_col = find_student_id_column(orig_df)

        # Build original student-ID -> value map
        if work_id_col and orig_id_col:
            orig_map = {
                normalize_student_id(row[orig_id_col]): row[orig_col]
                for _, row in orig_df.iterrows()
                if pd.notna(row[orig_id_col])
            }
            work_df[work_col] = work_df[work_col].astype(object)
            matched = 0
            for idx, row in work_df.iterrows():
                sid = normalize_student_id(row[work_id_col])
                if sid and sid in orig_map:
                    work_df.at[idx, work_col] = orig_map[sid]
                    matched += 1
            if verbose:
                print(f"      Matched {matched}/{len(work_df)} student(s) by ID")
        else:
            # Fallback: positional copy (same row order assumed)
            if verbose:
                print(f"      WARNING: No student ID column found — copying by position")
            work_df[work_col] = work_df[work_col].astype(object)
            work_df[work_col] = orig_df[orig_col].values

        recompute_averages(work_df, weights=load_weights_csv(weights_csv) if weights_csv else None)
        work_df.to_csv(lp, index=False, encoding='utf-8-sig', quoting=csv.QUOTE_ALL)
        if verbose:
            print(f"      Written: {lp}")
        reverted += 1

    print(f"\nReverted {reverted} gradebook(s).")
    # Append to audit log
    if audit_log is not None:
        # Supplement all_audit_records with simple "graded" records for any
        # assignment that had no due-date processing (no _apply_late_penalties call).
        for aname, df in processed:
            if aname in all_audit_records:
                continue
            from .common import find_name_columns
            email_col = find_email_column(df)
            id_col = find_student_id_column(df)
            score_col = next((c for c in df.columns if 'percent score' in c.lower()), None)
            first_col, last_col = find_name_columns(df)
            recs = []
            for _, row in df.iterrows():
                raw = row[score_col] if score_col else None
                if raw is None or pd.isna(raw):
                    continue
                try:
                    score_val = float(raw)
                except (TypeError, ValueError):
                    continue
                email = str(row[email_col]) if email_col and not pd.isna(row.get(email_col, float('nan'))) else ''
                sid = normalize_student_id(row[id_col]) if id_col else ''
                first = str(row[first_col]) if first_col and not pd.isna(row.get(first_col, float('nan'))) else ''
                last = str(row[last_col]) if last_col and not pd.isna(row.get(last_col, float('nan'))) else ''
                recs.append({
                    'Student ID': sid,
                    'School Email': email,
                    'Last Name': last,
                    'First Name': first,
                    'Score Date (local)': '',
                    'Due Date (local)': '',
                    'Delta': '',
                    'Status': 'graded',
                    'Penalty Factor': '',
                    'Original Score': round(score_val, 4),
                    'Applied Score': round(score_val, 4),
                })
            if recs:
                all_audit_records[aname] = recs

        lecture_file_list = list(lecture_dfs.keys()) if lecture_dfs else []
        args_dict = {
            'due_dates_csv': str(due_dates_csv) if due_dates_csv else None,
            'due': str(due) if due else None,
            'days_grace': days_grace,
            'hours_grace': hours_grace,
            'penalty': penalty,
            'grace_limit': grace_limit,
            'no_penalty_ids': sorted(no_penalty_ids) if no_penalty_ids else [],
        }
        for aname, recs in all_audit_records.items():
            log_records = []
            for rec in recs:
                pf_raw = rec.get('Penalty Factor')
                try:
                    pf = float(pf_raw)
                except (TypeError, ValueError):
                    status_val = rec.get('Status', '')
                    pf = 1.0 if status_val in ('on time', 'exempt', 'grace', 'graded') else None
                log_records.append({
                    'student_id': rec.get('Student ID') or '',
                    'username': extract_username_from_email(rec.get('School Email', '')) or '',
                    'name': f"{rec.get('Last Name', '')}, {rec.get('First Name', '')}".strip(', '),
                    'raw_score': rec.get('Original Score'),
                    'penalty_factor': pf,
                    'final_score': rec.get('Applied Score'),
                    'status': rec.get('Status', ''),
                    'score_date': rec.get('Score Date (local)') or None,
                    'how_late': rec.get('Delta') or None,
                })
            if log_records:
                audit_log.append_run(
                    command='assignment',
                    assignment=aname,
                    lecture_files=lecture_file_list,
                    args=args_dict,
                    records=log_records,
                )
        audit_log.save()
        print(f"\nAudit log updated: {audit_log.directory}")

    print("\nDone!")


def _find_component_columns(df):
    """
    Find component score columns in a zyBooks report DataFrame.

    Component columns have names starting with '<int>.<int>', e.g. '19.1 - Lab (10)'.
    Max points are parsed from the trailing '(N)' pattern.

    Returns:
        list of (col_name, max_pts_float) pairs, in order of appearance.
        Columns where max_pts cannot be parsed are excluded.
    """
    result = []
    for col in df.columns:
        if not re.match(r'^\d+\.\d+', col.strip()):
            continue
        m = re.search(r'\((\d+(?:\.\d+)?)\)\s*$', col.strip())
        if m:
            result.append((col, float(m.group(1))))
    return result


def _apply_best_one_of(df, verbose=True):
    """
    Replace each student's 'Percent score' with the best single-component percentage.

    For each student, compute score/max_pts*100 for every component column, then
    set 'Percent score' to the highest value.  Students with no valid component
    scores retain their original 'Percent score' value.
    """
    score_col = next((c for c in df.columns if 'percent score' in c.lower()), None)
    if not score_col:
        if verbose:
            print("   WARNING (--best-one-of): No 'Percent score' column found — skipping")
        return df

    components = _find_component_columns(df)
    if not components:
        if verbose:
            print("   WARNING (--best-one-of): No component columns found (e.g. '19.1 - Lab (10)')")
        return df

    if verbose:
        print(f"   best-one-of: {len(components)} component(s): "
              + ", ".join(c for c, _ in components))

    df[score_col] = df[score_col].astype(object)
    replaced = 0
    for idx, row in df.iterrows():
        best = None
        for col, _max_pts in components:
            val = row.get(col, '')
            if pd.isna(val) or str(val).strip() == '':
                continue
            try:
                score = float(val)
                if best is None or score > best:
                    best = score
            except ValueError:
                pass
        if best is not None:
            df.at[idx, score_col] = best
            replaced += 1

    if verbose:
        print(f"   best-one-of: replaced {replaced}/{len(df)} student score(s)")
    return df


def run_assignment(deadline_input, lecture_files=None, output_dir='.',
                   quiet=False, due_dates_csv=None, days_grace=0, hours_grace=0,
                   penalty=0.2, date_audit=False, force=False,
                   best_one_of=False, due=None, name=None, grace_limit=None,
                   no_penalty_ids=None, weights_csv=None, audit_log=None):
    """
    Process zyBooks assignment report CSV(s), apply late penalties from a
    due-dates table, and update BBLearn lecture gradebooks.

    Args:
        deadline_input: zyBooks assignment report CSV, or a directory of them
        lecture_files:  BBLearn gradebook CSVs to update (optional)
        output_dir:     Where to write updated gradebooks (default: '.')
        due_dates_csv:  CSV of per-week due dates (PA/CA/OL/IL60/... columns)
        days_grace:     Days after due date before late penalty applies
        hours_grace:    Additional hours (on top of days_grace) before penalty applies
        penalty:        Flat fraction deducted for any lateness beyond grace
        quiet:          Suppress verbose output
        best_one_of:    Replace Percent score with best single component percentage
        due:            Fallback due date string (used when due_dates_csv has no entry)
        name:           Override the derived assignment name (gradebook column target)
    """
    deadline_path = Path(deadline_input)
    if not deadline_path.exists():
        print(f"ERROR: Not found: {deadline_input}")
        sys.exit(1)

    print("\nAssignment Scorer")
    print("=" * 60)

    weights = load_weights_csv(weights_csv) if weights_csv else None

    due_dates = {}
    il_penalty_warned = False
    fallback_due_dt = None
    if due:
        try:
            fallback_due_dt = _parse_due_date_str(due)
        except Exception as e:
            print(f"ERROR: Could not parse --due '{due}': {e}")
            sys.exit(1)
    if due_dates_csv:
        due_dates = load_due_dates_csv(due_dates_csv)
        print(f"Loaded due dates: {len(due_dates)} entry(ies) from {Path(due_dates_csv).name}")
        print(f"Late penalty: {penalty * 100:.0f}%  |  Grace period: {days_grace}d {hours_grace}h")
    elif fallback_due_dt:
        print(f"Due date: {fallback_due_dt.tz_convert(_LOCAL_TZ).strftime('%Y-%m-%d %H:%M %Z')}")
        print(f"Late penalty: {penalty * 100:.0f}%  |  Grace period: {days_grace}d {hours_grace}h")

    if deadline_path.is_file():
        derived = assignment_name_from_path(deadline_path)
        aname = name if name else derived
        work_items = [(deadline_path, aname)]
        print(f"\nSingle file: {deadline_path.name}")
        if name:
            print(f"Assignment name override: '{name}'")
    else:
        if name:
            print(f"WARNING: --name is ignored when processing a directory")
        csvs = sorted(deadline_path.glob('*.csv'))
        if not csvs:
            print(f"\nERROR: No CSV files found in: {deadline_input}")
            sys.exit(1)
        work_items = [(f, assignment_name_from_path(f)) for f in csvs]
        print(f"\nFound {len(work_items)} file(s) in: {deadline_input}")

    verbose = not quiet

    # Load lecture gradebooks early — needed for lab section lookup (IL penalties)
    # and reused later for gradebook update.
    lecture_dfs = {}
    lecture_paths = {}
    if lecture_files:
        from .activity import apply_scores_to_gradebook
        from .common import build_student_score_maps, find_name_columns
        from .merge import find_username_column, sort_assignment_columns
        for lf in lecture_files:
            lf_path = Path(lf)
            if not lf_path.exists():
                print(f"ERROR: Gradebook not found: {lf}")
                continue
            lecture_dfs[lf_path.name] = read_csv_with_trailing_comma_fix(lf_path)
            lecture_paths[lf_path.name] = lf_path

    # Normalise exempt IDs for consistent comparison
    no_penalty_ids = (
        {normalize_student_id(sid) for sid in no_penalty_ids if normalize_student_id(sid)}
        if no_penalty_ids else None
    )

    # Build lab-section maps if IL due dates are present
    lab_section_map = {}
    id_section_map = {}
    if due_dates and lecture_dfs:
        has_il = any(t.startswith('IL') for (_, t) in due_dates)
        if has_il:
            lab_section_map, id_section_map = _build_lab_section_map(lecture_dfs)
            if verbose:
                print(f"Lab sections loaded: {len(lab_section_map)} student(s) with assigned sections")

    all_late_records = {}   # assignment_name -> list of late records
    all_audit_records = {}  # assignment_name -> list of audit records
    processed = []  # list of (assignment_name, df)

    for assignment_file, assignment_name in work_items:
        try:
            if verbose:
                print(f"\nProcessing: {assignment_name}  ({assignment_file.name})")
            df = read_csv_with_trailing_comma_fix(assignment_file)
            if verbose:
                print(f"   Students: {len(df)}")

            if best_one_of:
                df = _apply_best_one_of(df, verbose=verbose)

            if due_dates or fallback_due_dt:
                m = re.match(r'W(\d+)\s+(\S+)', assignment_name)
                if m:
                    week_num = int(m.group(1))
                    atype = m.group(2)
                    if atype == 'IL':
                        section_due_dates = {
                            t[2:]: dt
                            for (w, t), dt in due_dates.items()
                            if w == week_num and t.startswith('IL')
                        }
                        if not section_due_dates:
                            if verbose:
                                print(f"   NOTE: No IL due dates found for W{week_num}")
                        elif not lab_section_map:
                            print(f"   NOTE: No lab sections assigned — run assign-lab-section first")
                        else:
                            late_recs, audit_recs = _apply_late_penalties(
                                df, None, days_grace, penalty,
                                assignment_name, verbose=verbose,
                                lab_section_map=lab_section_map,
                                id_section_map=id_section_map,
                                section_due_dates=section_due_dates,
                                date_audit=date_audit,
                                hours_grace=hours_grace,
                                grace_limit=grace_limit,
                                no_penalty_ids=no_penalty_ids,
                            )
                            if late_recs:
                                all_late_records[assignment_name] = late_recs
                            if audit_recs:
                                all_audit_records[assignment_name] = audit_recs
                    else:
                        due_dt = due_dates.get((week_num, atype)) or fallback_due_dt
                        if due_dt:
                            late_recs, audit_recs = _apply_late_penalties(
                                df, due_dt, days_grace, penalty,
                                assignment_name, verbose=verbose,
                                date_audit=date_audit,
                                hours_grace=hours_grace,
                                grace_limit=grace_limit,
                                no_penalty_ids=no_penalty_ids,
                            )
                            if late_recs:
                                all_late_records[assignment_name] = late_recs
                            if audit_recs:
                                all_audit_records[assignment_name] = audit_recs
                        elif verbose:
                            print(f"   NOTE: No due date found for {assignment_name} — skipping penalty")
                elif fallback_due_dt:
                    # assignment name doesn't match W\d+ \S+ pattern; use fallback
                    late_recs, audit_recs = _apply_late_penalties(
                        df, fallback_due_dt, days_grace, penalty,
                        assignment_name, verbose=verbose,
                        date_audit=date_audit,
                        hours_grace=hours_grace,
                        grace_limit=grace_limit,
                        no_penalty_ids=no_penalty_ids,
                    )
                    if late_recs:
                        all_late_records[assignment_name] = late_recs
                    if audit_recs:
                        all_audit_records[assignment_name] = audit_recs

            processed.append((assignment_name, df))
        except Exception as e:
            print(f"\n   ERROR processing {assignment_name}: {e}")
            continue

    print("\n" + "=" * 60)
    print(f"Processed {len(processed)} assignment(s):")
    for aname, _ in processed:
        print(f"   - {aname}")

    out = Path(output_dir)
    late_dir   = out / 'late'
    audit_dir  = out / 'date_audit'
    orphan_dir = out / 'orphaned'

    if all_late_records:
        late_dir.mkdir(parents=True, exist_ok=True)
        print("\nLate submission reports:")
        for name, recs in all_late_records.items():
            safe_name = name.replace(' ', '_')
            late_path = late_dir / f'{safe_name}_late.csv'
            pd.DataFrame(recs).to_csv(late_path, index=False, encoding='utf-8-sig')
            print(f"   {name}: {len(recs)} student(s) -> {late_path}")
    else:
        print("\nNo late submissions.")

    if date_audit and all_audit_records:
        audit_dir.mkdir(parents=True, exist_ok=True)
        print("\nDate audit reports:")
        for name, recs in all_audit_records.items():
            safe_name = name.replace(' ', '_')
            audit_path = audit_dir / f'{safe_name}_date_audit.csv'
            pd.DataFrame(recs).to_csv(audit_path, index=False, encoding='utf-8-sig')
            print(f"   {name}: {len(recs)} student(s) -> {audit_path}")

    if lecture_dfs and processed:
        all_lecture_usernames = set()
        for lf_df in lecture_dfs.values():
            un_col = find_username_column(lf_df)
            em_col = find_email_column(lf_df)
            first_col, last_col = find_name_columns(lf_df)
            for _, row in lf_df.iterrows():
                if un_col and not pd.isna(row.get(un_col)):
                    all_lecture_usernames.add(str(row[un_col]).strip().lower())
                elif em_col:
                    u = extract_username_from_email(row[em_col])
                    if u:
                        all_lecture_usernames.add(u)
                if first_col and last_col:
                    first = str(row[first_col]).strip().lower() if pd.notna(row[first_col]) else ''
                    last = str(row[last_col]).strip().lower() if pd.notna(row[last_col]) else ''
                    if first and last:
                        all_lecture_usernames.add(f"{first}.{last}")

        print("\n" + "=" * 60)
        print("GRADEBOOK UPDATE")
        print("=" * 60)

        for assignment_name, df in processed:
            score_col = next(
                (c for c in df.columns if 'percent score' in c.lower()), None
            )
            if not score_col:
                print(f"\n   WARNING: No 'Percent score' column for '{assignment_name}' — skipping")
                continue

            score_map, id_map, name_map = build_student_score_maps(df, score_col)

            print(f"\n   Assignment: {assignment_name}")
            for lecture_name, lecture_df in lecture_dfs.items():
                try:
                    _, updated = apply_scores_to_gradebook(
                        lecture_df, score_map, assignment_name, verbose=verbose,
                        id_score_map=id_map, name_score_map=name_map, force=force,
                    )
                    if verbose:
                        print(f"      {lecture_name}: {updated} updated")
                except ValueError as e:
                    print(f"      ERROR ({lecture_name}): {e}")

            orphaned = [
                {'Username': u, 'Score': score_map[u]}
                for u in set(score_map.keys()) - all_lecture_usernames
                if not _middle_name_matched(u, all_lecture_usernames)
            ]
            if orphaned:
                orphan_dir.mkdir(parents=True, exist_ok=True)
                safe_name = assignment_name.replace(' ', '_')
                orphaned_path = orphan_dir / f'{safe_name}_orphaned.csv'
                pd.DataFrame(orphaned).to_csv(orphaned_path, index=False, encoding='utf-8-sig')
                print(f"      WARNING: {len(orphaned)} orphaned student(s) — see {orphaned_path}")

        out.mkdir(parents=True, exist_ok=True)
        print("\nWriting updated gradebooks:")
        for lecture_name, df in lecture_dfs.items():
            unnamed_cols = [col for col in df.columns if 'Unnamed' in str(col)]
            if unnamed_cols:
                df = df.drop(columns=unnamed_cols)
            df = sort_assignment_columns(df)
            lec_id_col = find_student_id_column(df)
            if lec_id_col:
                df[lec_id_col] = df[lec_id_col].apply(normalize_student_id)
            recompute_averages(df, weights=weights)
            output_path = out / lecture_paths[lecture_name].name
            df.to_csv(output_path, index=False, encoding='utf-8-sig', quoting=csv.QUOTE_ALL)
            print(f"   {output_path}")

    print("\nDone!")
