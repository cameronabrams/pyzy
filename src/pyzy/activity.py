"""
Activity Report Parser - processes zyBooks activity report CSV downloads
and merges per-student best scores into lecture section gradebooks.

Expected CSV columns:
    First name, Last name, Email, Class section,
    Date of submission, Score, Max score, Autograded test results
"""

import csv
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from .common import (
    build_student_score_maps,
    extract_username_from_email,
    find_email_column,
    find_name_columns,
    find_student_id_column,
    fmt_late,
    load_aliases_csv,
    load_weights_csv,
    middle_name_matched as _middle_name_matched,
    normalize_student_id,
    read_csv_with_trailing_comma_fix,
    recompute_averages,
    resolve_column,
)
from .merge import find_username_column, sort_assignment_columns


EXPECTED_COLUMNS = [
    'First name', 'Last name', 'Email', 'Class section',
    'Date of submission', 'Score', 'Max score', 'Autograded test results',
]


_LOCAL_TZ = ZoneInfo('America/New_York')


def _parse_due_date(due_date):
    """
    Parse a due date string into a UTC-aware Timestamp.
    Strings with a Z or UTC offset are parsed as-is.
    Strings without timezone info are treated as America/New_York local time.
    """
    dt = pd.to_datetime(due_date)
    if dt.tzinfo is None:
        dt = dt.tz_localize(_LOCAL_TZ)
    return dt.tz_convert('UTC')


def _late_penalty_factor(submission_dt, due_dt, grace_limit=None, days_grace=0, hours_grace=0, penalty=0.2):
    """
    Return the score multiplier for a submission relative to a due date.

    Grace window: days_grace days + hours_grace hours + 1 implicit hour.
    Beyond the grace window: flat deduction of `penalty`.
    If grace_limit set (days) and submission > grace_limit days late: 0.0.
    """
    delta_seconds = (submission_dt - due_dt).total_seconds()
    grace_seconds = days_grace * 86400 + hours_grace * 3600 + 3600  # +1 h always
    if delta_seconds <= grace_seconds:
        return 1.0
    if grace_limit is not None and delta_seconds > grace_limit * 86400:
        return 0.0
    return max(0.0, 1.0 - penalty)




def _print_late_report(late_df, due_dt):
    """Print a formatted table of late submissions."""
    rows = []
    for _, row in late_df.iterrows():
        delta_s = (row['_SubDt'] - due_dt).total_seconds()
        penalty_pct = round((1.0 - row['_PenaltyFactor']) * 100)
        penalized_pct = round(row['_PenalizedScore'] / row['Max score'] * 100, 1)
        rows.append({
            'Name': f"{row['Last name']}, {row['First name']}",
            'Email': row['Email'],
            'Submitted (UTC)': row['_SubDt'].strftime('%Y-%m-%d %H:%M'),
            'Late by': fmt_late(delta_s),
            'Penalty': f"-{penalty_pct}%",
            'Score': f"{row['Score']}/{int(row['Max score'])}",
            'After penalty': f"{penalized_pct}%",
        })

    rows.sort(key=lambda r: r['Name'])

    col_order = ['Name', 'Email', 'Submitted (UTC)', 'Late by', 'Penalty', 'Score', 'After penalty']
    widths = {c: max(len(c), max(len(r[c]) for r in rows)) for c in col_order}

    header = "  ".join(c.ljust(widths[c]) for c in col_order)
    rule = "  ".join("-" * widths[c] for c in col_order)
    print(f"\n   {'Late submission report':}")
    print(f"   {rule}")
    print(f"   {header}")
    print(f"   {rule}")
    for r in rows:
        print("   " + "  ".join(r[c].ljust(widths[c]) for c in col_order))
    print(f"   {rule}")


def parse_activity_report(filepath, verbose=True, due_date=None, select='max', grace_limit=None,
                          no_penalty_emails=None, days_grace=0, hours_grace=0, penalty=0.2,
                          aliases=None):
    """
    Parse a zyBooks activity report CSV and compute per-student scores.

    Args:
        filepath: Path to the activity report CSV
        verbose: Print progress information
        due_date: Optional due date (string or datetime-like).
                  For 'max'/'recent': a late penalty is applied to the selected
                  submission.
                  For 'pre-due': required — only submissions on or before this
                  datetime are considered; students with no on-time submission
                  receive a score of 0.
        select: Which submission per student counts:
                'max'      — highest raw score overall, then penalty applied (default)
                'recent'   — most recently submitted, then penalty applied
                'pre-due'  — highest score among on-time submissions only;
                             no penalty applied; requires --due

    Returns:
        DataFrame with one row per student, including a Percent column.
    """
    if select not in ('max', 'recent', 'pre-due'):
        raise ValueError(f"select must be 'max', 'recent', or 'pre-due', got '{select}'")
    if select == 'pre-due' and due_date is None:
        raise ValueError("--select pre-due requires --due to be set")

    df = pd.read_csv(filepath, encoding='utf-8-sig')

    missing = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(
            f"Missing expected columns: {', '.join(missing)}\n"
            f"Found columns: {', '.join(df.columns)}"
        )

    if verbose:
        print(f"   Loaded {len(df)} submissions from {len(df['Email'].unique())} students")

    df['_SubDt'] = pd.to_datetime(df['Date of submission'], utc=True)

    # --- pre-due: best on-time score; late submissions penalised (or zeroed past grace_limit) ---
    if select == 'pre-due':
        due_dt = _parse_due_date(due_date)
        due_dt_local = due_dt.tz_convert(_LOCAL_TZ)

        grace_seconds = days_grace * 86400 + hours_grace * 3600 + 3600  # +1 h always
        extended_due_dt = due_dt + pd.Timedelta(seconds=grace_seconds)

        on_time = df[df['_SubDt'] <= extended_due_dt]
        all_emails = set(df['Email'].unique())
        on_time_emails = set(on_time['Email'].unique())
        late_only_emails = all_emails - on_time_emails

        if verbose:
            print(f"   Selection: best on-time submission (pre-due)")
            print(f"   Due: {due_dt_local.strftime('%Y-%m-%d %H:%M %Z')}  |  "
                  f"On-time (within grace): {len(on_time_emails)}  |  "
                  f"Late (penalised): {len(late_only_emails)}")

        if len(on_time) > 0:
            best = df.loc[on_time.groupby('Email')['Score'].idxmax()].copy()
        else:
            best = pd.DataFrame(columns=df.columns)
        best['_PenaltyFactor'] = 1.0  # on-time: no penalty

        if late_only_emails:
            late_rows = df[df['Email'].isin(late_only_emails)].copy()
            # Pick the best-scoring submission for each late student
            late_rows = late_rows.loc[
                late_rows.groupby('Email')['Score'].idxmax()
            ].copy()
            late_rows['_LateOnly'] = True
            late_rows['_PenaltyFactor'] = [
                1.0 if (no_penalty_emails and extract_username_from_email(e) in no_penalty_emails)
                else _late_penalty_factor(s, due_dt, grace_limit, days_grace, hours_grace, penalty)
                for s, e in zip(late_rows['_SubDt'], late_rows['Email'])
            ]
            best = pd.concat([best, late_rows], ignore_index=True)
            best['_LateOnly'] = best['_LateOnly'].fillna(False)

        best['_PenalizedScore'] = best['Score'] * best['_PenaltyFactor']
        score_col = '_PenalizedScore'

    else:
        # --- max / recent ---
        if select == 'recent':
            idx = df.groupby('Email')['_SubDt'].idxmax()
            if verbose:
                print(f"   Selection: most recent submission")
        else:
            idx = df.groupby('Email')['Score'].idxmax()
            if verbose:
                print(f"   Selection: highest score")

        best = df.loc[idx].copy()

        # Apply late penalty
        if due_date is not None:
            due_dt = _parse_due_date(due_date)
            best['_PenaltyFactor'] = [
                1.0 if (no_penalty_emails and extract_username_from_email(e) in no_penalty_emails)
                else _late_penalty_factor(s, due_dt, grace_limit, days_grace, hours_grace, penalty)
                for s, e in zip(best['_SubDt'], best['Email'])
            ]
            best['_PenalizedScore'] = best['Score'] * best['_PenaltyFactor']
            score_col = '_PenalizedScore'

            late_mask = best['_SubDt'] > due_dt
            n_late = late_mask.sum()
            if verbose:
                print(f"   Due: {due_dt.strftime('%Y-%m-%d %H:%M UTC')}  |  Late: {n_late} student(s)")
                if n_late > 0:
                    _print_late_report(best[late_mask].copy(), due_dt)
        else:
            score_col = 'Score'

    best['Percent'] = (best[score_col] / best['Max score'] * 100).round(2)
    best['Username'] = best['Email'].apply(extract_username_from_email)
    if aliases:
        best['Username'] = best['Username'].apply(lambda u: aliases.get(u, u))
    best = best.sort_values(['Last name', 'First name']).reset_index(drop=True)

    if verbose:
        max_score = df['Max score'].iloc[0]
        mean_pct = best['Percent'].mean()
        print(f"   Max score: {max_score}")
        print(f"   Mean best score: {mean_pct:.1f}%")

    return best



def apply_scores_to_gradebook(df, score_map, column_pattern, verbose=True,
                               id_score_map=None, name_score_map=None, force=False):
    """
    Write scores from student lookup maps into a single gradebook DataFrame.

    Resolution order per gradebook row:
      1. Student ID  (if id_score_map provided)
      2. Username / email-derived username  (score_map)
      3. (Last name, First name) exact match  (if name_score_map provided)

    Args:
        df: Lecture gradebook DataFrame (modified in place)
        score_map: Dict mapping username -> score
        column_pattern: Substring identifying the target column (must match exactly one)
        verbose: Print detailed progress
        id_score_map: Optional dict mapping student_id -> score
        name_score_map: Optional dict mapping (last_lower, first_lower) -> score

    Returns:
        (column_name, rows_updated) tuple

    Raises:
        ValueError: If column_pattern does not match exactly one column.
    """
    column_name = resolve_column(df, column_pattern)
    if verbose and column_pattern != column_name:
        print(f"      Resolved '{column_pattern}' -> '{column_name}'")
    df[column_name] = df[column_name].astype(object)

    lec_email_col = find_email_column(df)
    lec_username_col = find_username_column(df)
    lec_id_col = find_student_id_column(df) if id_score_map else None
    lec_first_col, lec_last_col = find_name_columns(df) if name_score_map else (None, None)

    updated = 0
    name_matched = 0

    for idx, row in df.iterrows():
        score = None

        # 1. Student ID
        if lec_id_col and id_score_map:
            student_id = normalize_student_id(row[lec_id_col])
            if student_id and student_id in id_score_map:
                score = id_score_map[student_id]

        # 2. Username / email
        if score is None:
            username = None
            if lec_username_col and not pd.isna(row[lec_username_col]):
                username = str(row[lec_username_col]).strip().lower()
            elif lec_email_col:
                username = extract_username_from_email(row[lec_email_col])
            if username and username in score_map:
                score = score_map[username]

        # 3. (Last, First) name
        if score is None and name_score_map and lec_last_col and lec_first_col:
            last = str(row[lec_last_col]).strip().lower() if pd.notna(row[lec_last_col]) else ''
            first = str(row[lec_first_col]).strip().lower() if pd.notna(row[lec_first_col]) else ''
            if last and first and (last, first) in name_score_map:
                score = name_score_map[(last, first)]
                name_matched += 1

        if score is not None:
            existing = df.at[idx, column_name]
            try:
                existing_val = float(existing)
            except (ValueError, TypeError):
                existing_val = None
            if force or existing_val is None or score > existing_val:
                df.at[idx, column_name] = f"{score:.2f}"
                updated += 1

    if verbose and name_matched > 0:
        print(f"      Name-matched: {name_matched} student(s) — verify these")

    return column_name, updated


def run_activity(input_files, lecture_files, column_names, output_dir='.', quiet=False,
                 due_date=None, select='max', force=False, grace_limit=None, penalty=0.2,
                 weights_csv=None, no_penalty_ids=None, audit_log=None, days_grace=0, hours_grace=0,
                 aliases=None):
    """
    Run the activity report workflow for one or more reports.

    If a single column name is given for multiple reports, all reports are
    aggregated into that column: the best (max) score per student is kept
    and a "Number Submitted" count column is added (e.g. "3/9").

    If one column name per report is given, each report is written to its
    own column independently.

    Args:
        input_files: List of activity report CSV paths
        lecture_files: List of lecture section CSV paths
        column_names: List of column names (one per input, or exactly one for all)
        output_dir: Output directory for updated gradebooks
        quiet: Suppress verbose output
        due_date: Optional due date string; late submissions receive a penalty
        select: 'max' (highest raw score) or 'recent' (most recent submission)
    """
    aggregate = len(column_names) == 1 and len(input_files) > 1

    if not aggregate and len(input_files) != len(column_names):
        print(
            f"ERROR: Number of input files ({len(input_files)}) does not match "
            f"number of column names ({len(column_names)}). "
            f"Provide one name per file, or a single name to aggregate all."
        )
        sys.exit(1)

    for f in input_files:
        if not Path(f).exists():
            print(f"ERROR: File not found: {f}")
            sys.exit(1)

    for lf in lecture_files:
        if not Path(lf).exists():
            print(f"ERROR: File not found: {lf}")
            sys.exit(1)

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    verbose = not quiet

    print("\nActivity Report Merger")
    print("=" * 60)

    weights = load_weights_csv(weights_csv) if weights_csv else None
    norm_ids = {normalize_student_id(x) for x in no_penalty_ids} if no_penalty_ids else None
    args_dict = {
        'due_date': due_date,
        'select': select,
        'days_grace': days_grace,
        'hours_grace': hours_grace,
        'grace_limit': grace_limit,
        'penalty': penalty,
        'no_penalty_ids': sorted(norm_ids) if norm_ids else [],
    }
    if aggregate:
        _run_aggregated(input_files, lecture_files, column_names[0], out, verbose, due_date, select, force=force, grace_limit=grace_limit, penalty=penalty, weights=weights, no_penalty_ids=norm_ids, audit_log=audit_log, args_dict=args_dict, days_grace=days_grace, hours_grace=hours_grace, aliases=aliases)
    else:
        _run_per_column(input_files, lecture_files, column_names, out, verbose, due_date, select, force=force, grace_limit=grace_limit, penalty=penalty, weights=weights, no_penalty_ids=norm_ids, audit_log=audit_log, args_dict=args_dict, days_grace=days_grace, hours_grace=hours_grace, aliases=aliases)

    print("\nDone!")


def _username_to_id_map(gradebook_dfs):
    """Build {username: student_id} mapping from loaded gradebook DataFrames."""
    result = {}
    for df in gradebook_dfs.values():
        id_col = find_student_id_column(df)
        if not id_col:
            continue
        un_col = find_username_column(df)
        em_col = find_email_column(df)
        for _, row in df.iterrows():
            sid = normalize_student_id(row[id_col])
            if not sid:
                continue
            username = None
            if un_col and not pd.isna(row.get(un_col)):
                username = str(row[un_col]).strip().lower()
            elif em_col:
                username = extract_username_from_email(row[em_col])
            if username:
                result[username] = sid
    return result


def _username_to_section_map(gradebook_dfs):
    """Build {username: lab_section} and {student_id: lab_section} maps from gradebooks."""
    un_map = {}
    id_map = {}
    for df in gradebook_dfs.values():
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
                un_map[username] = section
            if id_col:
                sid = normalize_student_id(row[id_col])
                if sid:
                    id_map[sid] = section
    return un_map, id_map


def _build_audit_records(best, due_dt, u2id, select, days_grace=0, hours_grace=0):
    """
    Build audit log records (one per student) from the best-submission DataFrame.

    Args:
        best:    DataFrame returned by parse_activity_report
        due_dt:  UTC-aware Timestamp of the due date, or None
        u2id:    {username: student_id} mapping built from gradebooks
        select:  'max', 'recent', or 'pre-due'
    """
    records = []
    has_penalty = '_PenaltyFactor' in best.columns

    for _, row in best.iterrows():
        # Skip rows with no usable score (student not present in this report)
        if pd.isna(row.get('Score')) or pd.isna(row.get('Max score')):
            continue
        max_s = float(row['Max score'])
        if max_s == 0:
            continue

        username = row.get('Username') or extract_username_from_email(row['Email'])
        student_id = u2id.get(username, '') if username else ''
        name = f"{row['Last name']}, {row['First name']}"
        sub_dt = row.get('_SubDt')
        score_date = (
            sub_dt.tz_convert(_LOCAL_TZ).isoformat(timespec='minutes')
            if sub_dt is not None and not pd.isna(sub_dt) else None
        )

        if has_penalty and due_dt is not None:
            grace_seconds = days_grace * 86400 + hours_grace * 3600 + 3600  # +1 h always
            factor = float(row.get('_PenaltyFactor', 1.0))
            raw_score = float(row['Score']) / max_s * 100
            final_score = float(row.get('_PenalizedScore', row['Score'])) / max_s * 100
            delta_s = (sub_dt - due_dt).total_seconds() if sub_dt is not None else 0
            if delta_s > grace_seconds:
                status = 'exempt' if factor >= 1.0 else 'late'
                how_late = fmt_late(delta_s)
            else:
                status = 'on_time'
                how_late = None
        else:
            factor = 1.0
            raw_score = float(row['Score']) / max_s * 100
            final_score = float(row['Percent'])
            status = 'on_time' if due_dt is not None else 'graded'
            how_late = None

        records.append({
            'student_id': student_id,
            'username': username or '',
            'name': name,
            'raw_score': round(raw_score, 4),
            'penalty_factor': round(factor, 4),
            'final_score': round(final_score, 4),
            'status': status,
            'score_date': score_date,
            'how_late': how_late,
        })
    return records


def _dedup_late_records(records):
    """
    Deduplicate late records by student email, keeping the one with the
    highest Applied Score (i.e. the activity that counted toward their grade).
    """
    by_email = {}
    for rec in records:
        email = rec.get('School Email', '')
        prev = by_email.get(email)
        if prev is None:
            by_email[email] = rec
        else:
            try:
                curr = float(rec.get('Original Score') or 0)
                best_so_far = float(prev.get('Original Score') or 0)
                if curr > best_so_far:
                    by_email[email] = rec
            except (TypeError, ValueError):
                pass
    return list(by_email.values())


def _build_late_records(best, due_dt, gradebook_dfs, days_grace=0, hours_grace=0):
    """
    Build late submission records from a best-submission DataFrame.

    Returns a list of dicts with the same columns as the assignment late report.
    Works for both max/recent mode (_PenaltyFactor present) and pre-due mode
    (_LateOnly present).
    """
    username_to_id = _username_to_id_map(gradebook_dfs)
    un_to_section, id_to_section = _username_to_section_map(gradebook_dfs)
    grace_seconds = days_grace * 86400 + hours_grace * 3600 + 3600  # +1 h always

    records = []

    if '_PenaltyFactor' in best.columns:
        for _, row in best.iterrows():
            if pd.isna(row.get('_SubDt')):
                continue
            delta_s = (row['_SubDt'] - due_dt).total_seconds()
            if delta_s <= grace_seconds:  # within grace window — not actually late
                continue
            max_s = float(row['Max score']) or 1.0
            original_pct = float(row['Score']) / max_s * 100
            penalized_pct = float(row['_PenalizedScore']) / max_s * 100
            username = extract_username_from_email(row['Email'])
            sid = username_to_id.get(username, '')
            section = un_to_section.get(username) or id_to_section.get(sid, '')
            records.append({
                'Last Name': row['Last name'],
                'First Name': row['First name'],
                'Student ID': sid,
                'Lab Section': section,
                'School Email': row['Email'],
                'Score Date': row['_SubDt'].strftime('%Y-%m-%d %H:%M UTC'),
                'How Late': fmt_late(delta_s),
                'Original Score': round(original_pct, 4),
                'Penalty Factor': round(float(row['_PenaltyFactor']), 4),
                'Applied Score': round(penalized_pct, 4),
            })

    return records


def _exempt_usernames_from_ids(gradebook_dfs, no_penalty_ids):
    """
    Build the set of usernames exempt from late penalties.

    Scans gradebook DataFrames for rows matching any ID in no_penalty_ids and
    collects the corresponding username/email-derived username.
    """
    if not no_penalty_ids:
        return set()
    exempt = set()
    for df in gradebook_dfs.values():
        id_col = find_student_id_column(df)
        un_col = find_username_column(df)
        em_col = find_email_column(df)
        if not id_col:
            continue
        for _, row in df.iterrows():
            sid = normalize_student_id(row[id_col])
            if sid and sid in no_penalty_ids:
                username = None
                if un_col and not pd.isna(row.get(un_col)):
                    username = str(row[un_col]).strip().lower()
                elif em_col:
                    username = extract_username_from_email(row[em_col])
                if username:
                    exempt.add(username)
    return exempt


def _gradebook_usernames(gradebook_dfs):
    """
    Return the set of all usernames present in any of the loaded gradebook
    DataFrames, plus synthesised first.last keys derived from name columns
    (used to suppress false-positive orphan reports for first.middle.last
    zyBooks usernames whose shortened form matches a known student by name).
    """
    usernames = set()
    for df in gradebook_dfs.values():
        un_col = find_username_column(df)
        em_col = find_email_column(df)
        first_col, last_col = find_name_columns(df)
        for _, row in df.iterrows():
            if un_col and not pd.isna(row.get(un_col)):
                usernames.add(str(row[un_col]).strip().lower())
            elif em_col:
                u = extract_username_from_email(row[em_col])
                if u:
                    usernames.add(u)
            if first_col and last_col:
                first = str(row[first_col]).strip().lower() if pd.notna(row[first_col]) else ''
                last = str(row[last_col]).strip().lower() if pd.notna(row[last_col]) else ''
                if first and last:
                    usernames.add(f"{first}.{last}")
    return usernames


def _write_orphan_report(score_map, all_lecture_usernames, label, out):
    """Write orphaned students (in score_map but not in any gradebook) to a CSV."""
    orphaned = [
        {'Username': u, 'Score': score_map[u]}
        for u in sorted(set(score_map.keys()) - all_lecture_usernames)
        if not _middle_name_matched(u, all_lecture_usernames)
    ]
    if orphaned:
        safe_label = label.replace(' ', '_')
        orphaned_path = out / f'{safe_label}_orphaned.csv'
        pd.DataFrame(orphaned).to_csv(orphaned_path, index=False, encoding='utf-8-sig')
        print(f"      WARNING: {len(orphaned)} orphaned student(s) not found "
              f"in any gradebook — see {orphaned_path}")


def _run_aggregated(input_files, lecture_files, column_name, out, verbose, due_date=None, select='max', force=False, grace_limit=None, penalty=0.2, weights=None, no_penalty_ids=None, audit_log=None, args_dict=None, days_grace=0, hours_grace=0, aliases=None):
    """Aggregate multiple reports into a single column, keeping max score per student."""
    n_activities = len(input_files)

    # Load gradebooks early so we can build the exempt-username set
    lecture_paths = {Path(fp).name: Path(fp) for fp in lecture_files}
    gradebook_dfs = {name: read_csv_with_trailing_comma_fix(fp) for name, fp in lecture_paths.items()}
    all_lecture_usernames = _gradebook_usernames(gradebook_dfs)
    exempt_usernames = _exempt_usernames_from_ids(gradebook_dfs, no_penalty_ids) or None
    u2id = _username_to_id_map(gradebook_dfs)

    # username -> {max_score, count}
    combined = {}
    name_combined = {}  # (last_lower, first_lower) -> max_score
    all_late_recs_flat = []  # accumulate across all input files; deduplicated at write time

    due_dt = _parse_due_date(due_date) if due_date is not None else None

    for input_file in input_files:
        filepath = Path(input_file)
        print(f"\n   Report: {filepath.name}")

        best = parse_activity_report(filepath, verbose=verbose, due_date=due_date, select=select, grace_limit=grace_limit, no_penalty_emails=exempt_usernames, days_grace=days_grace, hours_grace=hours_grace, penalty=penalty, aliases=aliases)

        if due_dt is not None:
            recs = _build_late_records(best, due_dt, gradebook_dfs, days_grace=days_grace, hours_grace=hours_grace)
            all_late_recs_flat.extend(recs)

        if audit_log is not None:
            arecs = _build_audit_records(best, due_dt, u2id, select, days_grace=days_grace, hours_grace=hours_grace)
            if arecs:
                audit_log.append_run(
                    command='activity',
                    assignment=column_name,
                    lecture_files=list(lecture_paths.keys()),
                    args={**(args_dict or {}), 'source_file': filepath.name},
                    records=arecs,
                )

        first_col, last_col = find_name_columns(best)

        for _, row in best.iterrows():
            username = row['Username']
            pct = row['Percent']
            raw_pct = (row['Score'] / row['Max score'] * 100) if row['Max score'] else 0

            if username:
                if username in combined:
                    combined[username]['count'] += 1
                    if raw_pct > combined[username]['raw_pct']:
                        combined[username]['raw_pct'] = raw_pct
                        combined[username]['max_score'] = pct
                else:
                    combined[username] = {'raw_pct': raw_pct, 'max_score': pct, 'count': 1}

            if first_col and last_col:
                first = str(row[first_col]).strip().lower() if pd.notna(row[first_col]) else ''
                last = str(row[last_col]).strip().lower() if pd.notna(row[last_col]) else ''
                if first and last:
                    key = (last, first)
                    if key not in name_combined or raw_pct > name_combined[key]['raw_pct']:
                        name_combined[key] = {'raw_pct': raw_pct, 'score': pct}

    if verbose:
        print(f"\n   Aggregated {len(combined)} students across {n_activities} reports")

    score_map = {u: v['max_score'] for u, v in combined.items()}
    count_map = {u: v['count'] for u, v in combined.items()}
    name_score_map = {k: v['score'] for k, v in name_combined.items()}

    if all_late_recs_flat:
        deduped = _dedup_late_records(all_late_recs_flat)
        late_dir = out / 'late'
        late_dir.mkdir(parents=True, exist_ok=True)
        safe_name = column_name.replace(' ', '_')
        late_path = late_dir / f'{safe_name}_late.csv'
        pd.DataFrame(deduped).to_csv(late_path, index=False, encoding='utf-8-sig')
        print(f"\nLate submission report: {len(deduped)} student(s) -> {late_path}")
    elif due_date is not None:
        print("\nNo late submissions.")

    print("\nWriting output files:")
    for lecture_name, df in gradebook_dfs.items():
        if verbose:
            print(f"\n   -> {lecture_name} ({len(df)} students)")

        resolved_name, updated = apply_scores_to_gradebook(
            df, score_map, column_name, verbose=verbose,
            name_score_map=name_score_map, force=force,
        )
        count_col = "Number Submitted"

        # Add submission count column
        lec_email_col = find_email_column(df)
        lec_username_col = find_username_column(df)
        df[count_col] = ''
        for idx, row in df.iterrows():
            username = None
            if lec_username_col and not pd.isna(row[lec_username_col]):
                username = str(row[lec_username_col]).strip().lower()
            elif lec_email_col:
                username = extract_username_from_email(row[lec_email_col])
            if username and username in count_map:
                df.at[idx, count_col] = f"{count_map[username]}/{n_activities}"

        if verbose:
            print(f"      Updated: {updated}")

        unnamed_cols = [col for col in df.columns if 'Unnamed' in str(col)]
        if unnamed_cols:
            df = df.drop(columns=unnamed_cols)
        df = sort_assignment_columns(df)

        recompute_averages(df, weights=weights)
        output_path = lecture_paths[lecture_name]
        df.to_csv(output_path, index=False, encoding='utf-8-sig', quoting=csv.QUOTE_ALL)
        print(f"   {output_path}")

    _write_orphan_report(score_map, all_lecture_usernames, column_name, out)

    if audit_log is not None:
        audit_log.save()
        print(f"\nAudit log updated: {audit_log.directory}")


def _run_per_column(input_files, lecture_files, column_names, out, verbose, due_date=None, select='max', force=False, grace_limit=None, penalty=0.2, weights=None, no_penalty_ids=None, audit_log=None, args_dict=None, days_grace=0, hours_grace=0, aliases=None):
    """Each report gets its own column in the gradebook."""
    lecture_paths = {Path(fp).name: Path(fp) for fp in lecture_files}
    gradebooks = {name: read_csv_with_trailing_comma_fix(fp) for name, fp in lecture_paths.items()}

    all_lecture_usernames = _gradebook_usernames(gradebooks)
    exempt_usernames = _exempt_usernames_from_ids(gradebooks, no_penalty_ids) or None
    u2id = _username_to_id_map(gradebooks)

    all_late_records = {}  # column_name -> list of late record dicts
    due_dt = _parse_due_date(due_date) if due_date is not None else None

    for input_file, column_name in zip(input_files, column_names):
        filepath = Path(input_file)
        print(f"\n   Report: {filepath.name}  ->  column '{column_name}'")

        best = parse_activity_report(filepath, verbose=verbose, due_date=due_date, select=select, grace_limit=grace_limit, no_penalty_emails=exempt_usernames, days_grace=days_grace, hours_grace=hours_grace, penalty=penalty, aliases=aliases)

        if due_dt is not None:
            recs = _build_late_records(best, due_dt, gradebooks, days_grace=days_grace, hours_grace=hours_grace)
            if recs:
                all_late_records[column_name] = recs

        if audit_log is not None:
            arecs = _build_audit_records(best, due_dt, u2id, select, days_grace=days_grace, hours_grace=hours_grace)
            if arecs:
                audit_log.append_run(
                    command='activity',
                    assignment=column_name,
                    lecture_files=list(lecture_paths.keys()),
                    args={**(args_dict or {}), 'source_file': filepath.name},
                    records=arecs,
                )

        score_map, id_map, name_map = build_student_score_maps(best, 'Percent')

        for lecture_name, df in gradebooks.items():
            if verbose:
                print(f"\n   -> {lecture_name} ({len(df)} students)")
            _, updated = apply_scores_to_gradebook(
                df, score_map, column_name, verbose=verbose,
                id_score_map=id_map, name_score_map=name_map, force=force,
            )
            if verbose:
                print(f"      Updated: {updated}")

        _write_orphan_report(score_map, all_lecture_usernames, column_name, out)

    if all_late_records:
        late_dir = out / 'late'
        late_dir.mkdir(parents=True, exist_ok=True)
        print("\nLate submission reports:")
        for col, recs in all_late_records.items():
            safe_name = col.replace(' ', '_')
            late_path = late_dir / f'{safe_name}_late.csv'
            pd.DataFrame(recs).to_csv(late_path, index=False, encoding='utf-8-sig')
            print(f"   {col}: {len(recs)} student(s) -> {late_path}")
    elif due_date is not None:
        print("\nNo late submissions.")

    print("\nWriting output files:")
    for lecture_name, df in gradebooks.items():
        unnamed_cols = [col for col in df.columns if 'Unnamed' in str(col)]
        if unnamed_cols:
            df = df.drop(columns=unnamed_cols)
        df = sort_assignment_columns(df)

        recompute_averages(df, weights=weights)
        output_path = lecture_paths[lecture_name]
        df.to_csv(output_path, index=False, encoding='utf-8-sig', quoting=csv.QUOTE_ALL)
        print(f"   {output_path}")

    if audit_log is not None:
        audit_log.save()
        print(f"\nAudit log updated: {audit_log.directory}")
