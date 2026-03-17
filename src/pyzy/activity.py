"""
Activity Report Parser - processes zyBooks activity report CSV downloads
and merges per-student best scores into lecture section gradebooks.

Expected CSV columns:
    First name, Last name, Email, Class section,
    Date of submission, Score, Max score, Autograded test results
"""

import math
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
    normalize_student_id,
    read_csv_with_trailing_comma_fix,
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


def _late_penalty_factor(submission_dt, due_dt):
    """
    Return the score multiplier for a submission relative to a due date.

    - On time or ≤ 1 h late: 1.0 (no deduction)
    - 1 h – 24 h late: 0.80  (−20%)
    - 24 h – 48 h late: 0.70  (−30%)
    - Each additional 24 h: −10 pp, floored at 0.0
    """
    delta_seconds = (submission_dt - due_dt).total_seconds()
    if delta_seconds <= 3600:
        return 1.0
    days_late = math.ceil((delta_seconds - 3600) / 86400)
    penalty = 0.20 + 0.10 * max(0, days_late - 1)
    return max(0.0, 1.0 - penalty)


def _fmt_late(delta_seconds):
    """Format a positive number of seconds as e.g. '2d 3h 15m'."""
    total_minutes = int(delta_seconds // 60)
    days, remainder = divmod(total_minutes, 1440)
    hours, minutes = divmod(remainder, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    parts.append(f"{minutes}m")
    return " ".join(parts)


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
            'Late by': _fmt_late(delta_s),
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


def parse_activity_report(filepath, verbose=True, due_date=None, select='max'):
    """
    Parse a zyBooks activity report CSV and compute per-student scores.

    Args:
        filepath: Path to the activity report CSV
        verbose: Print progress information
        due_date: Optional due date (string or datetime-like). When provided,
                  a late penalty is applied to the selected submission.
                  ISO 8601 strings are accepted (e.g. "2026-03-09T23:59:00Z"
                  or "2026-03-09 23:59"); strings without a timezone offset
                  are treated as UTC.
        select: Which submission per student counts — 'max' (highest raw score,
                default) or 'recent' (most recently submitted).

    Returns:
        DataFrame with one row per student, including a Percent column.
    """
    if select not in ('max', 'recent'):
        raise ValueError(f"select must be 'max' or 'recent', got '{select}'")

    df = pd.read_csv(filepath, encoding='utf-8-sig')

    missing = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(
            f"Missing expected columns: {', '.join(missing)}\n"
            f"Found columns: {', '.join(df.columns)}"
        )

    if verbose:
        print(f"   Loaded {len(df)} submissions from {len(df['Email'].unique())} students")

    # Parse submission timestamps (always needed: for 'recent' selection and late detection)
    df['_SubDt'] = pd.to_datetime(df['Date of submission'], utc=True)

    # Select one submission per student
    if select == 'recent':
        idx = df.groupby('Email')['_SubDt'].idxmax()
        if verbose:
            print(f"   Selection: most recent submission")
    else:
        idx = df.groupby('Email')['Score'].idxmax()
        if verbose:
            print(f"   Selection: highest score")

    best = df.loc[idx].copy()

    # Apply late penalty to the selected submission
    if due_date is not None:
        due_dt = _parse_due_date(due_date)
        best['_PenaltyFactor'] = [_late_penalty_factor(s, due_dt) for s in best['_SubDt']]
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
    best = best.sort_values(['Last name', 'First name']).reset_index(drop=True)

    if verbose:
        max_score = df['Max score'].iloc[0]
        mean_pct = best['Percent'].mean()
        print(f"   Max score: {max_score}")
        print(f"   Mean best score: {mean_pct:.1f}%")

    return best



def apply_scores_to_gradebook(df, score_map, column_pattern, verbose=True,
                               id_score_map=None, name_score_map=None):
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
            df.at[idx, column_name] = score
            updated += 1

    if verbose and name_matched > 0:
        print(f"      Name-matched: {name_matched} student(s) — verify these")

    return column_name, updated


def run_activity(input_files, lecture_files, column_names, output_dir='.', quiet=False,
                 due_date=None, select='max'):
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

    if aggregate:
        _run_aggregated(input_files, lecture_files, column_names[0], out, verbose, due_date, select)
    else:
        _run_per_column(input_files, lecture_files, column_names, out, verbose, due_date, select)

    print("\nDone!")


def _gradebook_usernames(gradebook_dfs):
    """Return the set of all usernames present in any of the loaded gradebook DataFrames."""
    usernames = set()
    for df in gradebook_dfs.values():
        un_col = find_username_column(df)
        em_col = find_email_column(df)
        for _, row in df.iterrows():
            if un_col and not pd.isna(row.get(un_col)):
                usernames.add(str(row[un_col]).strip().lower())
            elif em_col:
                u = extract_username_from_email(row[em_col])
                if u:
                    usernames.add(u)
    return usernames


def _write_orphan_report(score_map, all_lecture_usernames, label, out):
    """Write orphaned students (in score_map but not in any gradebook) to a CSV."""
    orphaned = [
        {'Username': u, 'Score': score_map[u]}
        for u in sorted(set(score_map.keys()) - all_lecture_usernames)
    ]
    if orphaned:
        safe_label = label.replace(' ', '_')
        orphaned_path = out / f'{safe_label}_orphaned.csv'
        pd.DataFrame(orphaned).to_csv(orphaned_path, index=False, encoding='utf-8-sig')
        print(f"      WARNING: {len(orphaned)} orphaned student(s) not found "
              f"in any gradebook — see {orphaned_path}")


def _run_aggregated(input_files, lecture_files, column_name, out, verbose, due_date=None, select='max'):
    """Aggregate multiple reports into a single column, keeping max score per student."""
    n_activities = len(input_files)

    # username -> {max_score, count}
    combined = {}
    name_combined = {}  # (last_lower, first_lower) -> max_score

    for input_file in input_files:
        filepath = Path(input_file)
        print(f"\n   Report: {filepath.name}")

        best = parse_activity_report(filepath, verbose=verbose, due_date=due_date, select=select)

        first_col, last_col = find_name_columns(best)

        for _, row in best.iterrows():
            username = row['Username']
            pct = row['Percent']

            if username:
                if username in combined:
                    combined[username]['max_score'] = max(combined[username]['max_score'], pct)
                    combined[username]['count'] += 1
                else:
                    combined[username] = {'max_score': pct, 'count': 1}

            if first_col and last_col:
                first = str(row[first_col]).strip().lower() if pd.notna(row[first_col]) else ''
                last = str(row[last_col]).strip().lower() if pd.notna(row[last_col]) else ''
                if first and last:
                    key = (last, first)
                    name_combined[key] = max(name_combined.get(key, 0), pct)

    if verbose:
        print(f"\n   Aggregated {len(combined)} students across {n_activities} reports")

    score_map = {u: v['max_score'] for u, v in combined.items()}
    count_map = {u: v['count'] for u, v in combined.items()}
    name_score_map = name_combined

    # Load gradebooks, apply, and write
    gradebook_dfs = {
        Path(fp).name: read_csv_with_trailing_comma_fix(fp)
        for fp in lecture_files
    }
    all_lecture_usernames = _gradebook_usernames(gradebook_dfs)

    print("\nWriting output files:")
    for lecture_name, df in gradebook_dfs.items():
        if verbose:
            print(f"\n   -> {lecture_name} ({len(df)} students)")

        resolved_name, updated = apply_scores_to_gradebook(
            df, score_map, column_name, verbose=verbose,
            name_score_map=name_score_map,
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

        output_name = lecture_name.replace('.csv', '_merged.csv')
        output_path = out / output_name
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"   {output_path}")

    _write_orphan_report(score_map, all_lecture_usernames, column_name, out)


def _run_per_column(input_files, lecture_files, column_names, out, verbose, due_date=None, select='max'):
    """Each report gets its own column in the gradebook."""
    gradebooks = {}
    for filepath in lecture_files:
        name = Path(filepath).name
        gradebooks[name] = read_csv_with_trailing_comma_fix(filepath)

    all_lecture_usernames = _gradebook_usernames(gradebooks)

    for input_file, column_name in zip(input_files, column_names):
        filepath = Path(input_file)
        print(f"\n   Report: {filepath.name}  ->  column '{column_name}'")

        best = parse_activity_report(filepath, verbose=verbose, due_date=due_date, select=select)

        score_map, id_map, name_map = build_student_score_maps(best, 'Percent')

        for lecture_name, df in gradebooks.items():
            if verbose:
                print(f"\n   -> {lecture_name} ({len(df)} students)")
            _, updated = apply_scores_to_gradebook(
                df, score_map, column_name, verbose=verbose,
                id_score_map=id_map, name_score_map=name_map,
            )
            if verbose:
                print(f"      Updated: {updated}")

        _write_orphan_report(score_map, all_lecture_usernames, column_name, out)

    print("\nWriting output files:")
    for lecture_name, df in gradebooks.items():
        unnamed_cols = [col for col in df.columns if 'Unnamed' in str(col)]
        if unnamed_cols:
            df = df.drop(columns=unnamed_cols)
        df = sort_assignment_columns(df)

        output_name = lecture_name.replace('.csv', '_merged.csv')
        output_path = out / output_name
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"   {output_path}")
