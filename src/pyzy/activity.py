"""
Activity Report Parser - processes zyBooks activity report CSV downloads
and merges per-student best scores into lecture section gradebooks.

Expected CSV columns:
    First name, Last name, Email, Class section,
    Date of submission, Score, Max score, Autograded test results
"""

import sys
from pathlib import Path

import pandas as pd

from .common import (
    extract_username_from_email,
    find_email_column,
    find_student_id_column,
    normalize_student_id,
    read_csv_with_trailing_comma_fix,
)
from .merge import find_username_column, sort_assignment_columns


EXPECTED_COLUMNS = [
    'First name', 'Last name', 'Email', 'Class section',
    'Date of submission', 'Score', 'Max score', 'Autograded test results',
]


def parse_activity_report(filepath, verbose=True):
    """
    Parse a zyBooks activity report CSV and compute per-student best scores.

    Returns:
        DataFrame with one row per student, including a Percent column.
    """
    df = pd.read_csv(filepath, encoding='utf-8-sig')

    missing = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(
            f"Missing expected columns: {', '.join(missing)}\n"
            f"Found columns: {', '.join(df.columns)}"
        )

    if verbose:
        print(f"   Loaded {len(df)} submissions from {len(df['Email'].unique())} students")

    # Best score per student
    idx = df.groupby('Email')['Score'].idxmax()
    best = df.loc[idx].copy()
    best['Percent'] = (best['Score'] / best['Max score'] * 100).round(2)
    best['Username'] = best['Email'].apply(extract_username_from_email)
    best = best.sort_values(['Last name', 'First name']).reset_index(drop=True)

    if verbose:
        max_score = df['Max score'].iloc[0]
        mean_pct = best['Percent'].mean()
        print(f"   Max score: {max_score}")
        print(f"   Mean best score: {mean_pct:.1f}%")

    return best


def apply_scores_to_gradebook(df, score_map, column_name, verbose=True):
    """
    Write scores from a username->percent map into a single gradebook DataFrame.

    Args:
        df: Lecture gradebook DataFrame (modified in place)
        score_map: Dict mapping username -> percent score
        column_name: Column name to write (e.g. "W1 CA")
        verbose: Print detailed progress

    Returns:
        Number of rows updated
    """
    lec_email_col = find_email_column(df)
    lec_username_col = find_username_column(df)

    if column_name not in df.columns:
        df[column_name] = ''
        if verbose:
            print(f"      Created column: '{column_name}'")

    updated = 0
    for idx, row in df.iterrows():
        username = None
        if lec_username_col and not pd.isna(row[lec_username_col]):
            username = str(row[lec_username_col]).strip().lower()
        elif lec_email_col:
            username = extract_username_from_email(row[lec_email_col])

        if username and username in score_map:
            df.at[idx, column_name] = score_map[username]
            updated += 1

    return updated


def run_activity(input_files, lecture_files, column_names, output_dir='.', quiet=False):
    """
    Run the activity report workflow for one or more reports.

    If a single column name is given for multiple reports, all reports are
    aggregated into that column: the best (max) score per student is kept
    and a "<column> Submitted" count column is added (e.g. "3/9").

    If one column name per report is given, each report is written to its
    own column independently.

    Args:
        input_files: List of activity report CSV paths
        lecture_files: List of lecture section CSV paths
        column_names: List of column names (one per input, or exactly one for all)
        output_dir: Output directory for updated gradebooks
        quiet: Suppress verbose output
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
        _run_aggregated(input_files, lecture_files, column_names[0], out, verbose)
    else:
        _run_per_column(input_files, lecture_files, column_names, out, verbose)

    print("\nDone!")


def _run_aggregated(input_files, lecture_files, column_name, out, verbose):
    """Aggregate multiple reports into a single column, keeping max score per student."""
    n_activities = len(input_files)
    count_col = f"{column_name} Submitted"

    # username -> {max_score, count}
    combined = {}

    for input_file in input_files:
        filepath = Path(input_file)
        print(f"\n   Report: {filepath.name}")

        best = parse_activity_report(filepath, verbose=verbose)

        for _, row in best.iterrows():
            username = row['Username']
            if not username:
                continue
            pct = row['Percent']
            if username in combined:
                combined[username]['max_score'] = max(combined[username]['max_score'], pct)
                combined[username]['count'] += 1
            else:
                combined[username] = {'max_score': pct, 'count': 1}

    if verbose:
        print(f"\n   Aggregated {len(combined)} students across {n_activities} reports")

    score_map = {u: v['max_score'] for u, v in combined.items()}
    count_map = {u: v['count'] for u, v in combined.items()}

    # Load gradebooks, apply, and write
    print("\nWriting output files:")
    for filepath in lecture_files:
        lecture_name = Path(filepath).name
        df = read_csv_with_trailing_comma_fix(filepath)

        if verbose:
            print(f"\n   -> {lecture_name} ({len(df)} students)")

        updated = apply_scores_to_gradebook(df, score_map, column_name, verbose=verbose)

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


def _run_per_column(input_files, lecture_files, column_names, out, verbose):
    """Each report gets its own column in the gradebook."""
    gradebooks = {}
    for filepath in lecture_files:
        name = Path(filepath).name
        gradebooks[name] = read_csv_with_trailing_comma_fix(filepath)

    for input_file, column_name in zip(input_files, column_names):
        filepath = Path(input_file)
        print(f"\n   Report: {filepath.name}  ->  column '{column_name}'")

        best = parse_activity_report(filepath, verbose=verbose)

        score_map = {}
        for _, row in best.iterrows():
            username = row['Username']
            if username:
                score_map[username] = row['Percent']

        for lecture_name, df in gradebooks.items():
            if verbose:
                print(f"\n   -> {lecture_name} ({len(df)} students)")
            updated = apply_scores_to_gradebook(df, score_map, column_name, verbose=verbose)
            if verbose:
                print(f"      Updated: {updated}")

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
