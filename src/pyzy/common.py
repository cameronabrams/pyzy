"""Shared utilities for CSV parsing, student ID matching, and assignment name parsing."""

import io
import re
from pathlib import Path

import pandas as pd


def read_csv_with_trailing_comma_fix(filepath):
    """
    Read a CSV file that may have trailing commas on each line.

    Args:
        filepath: Path to CSV file

    Returns:
        pandas DataFrame
    """
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        lines = f.readlines()

    fixed_lines = []
    for line in lines:
        line = line.rstrip('\n\r')
        if line.endswith(','):
            line = line[:-1]
        fixed_lines.append(line + '\n')

    return pd.read_csv(io.StringIO(''.join(fixed_lines)))


def normalize_student_id(student_id):
    """
    Normalize student ID to a string, handling float/int conversions.

    Examples:
        14788528.0 -> "14788528"
        14788528 -> "14788528"
        "14788528" -> "14788528"
    """
    if pd.isna(student_id):
        return None

    id_str = str(student_id).strip()

    if '.' in id_str:
        try:
            id_float = float(id_str)
            if id_float == int(id_float):
                return str(int(id_float))
        except (ValueError, OverflowError):
            pass

    return id_str


def find_student_id_column(df):
    """Find the student ID column in a DataFrame."""
    patterns = [
        'student id', 'studentid', 'student_id', 'sid',
        'id', 'student number', 'student_number'
    ]

    for col in df.columns:
        col_lower = col.lower().replace(' ', '').replace('_', '')
        for pattern in patterns:
            pattern_clean = pattern.replace(' ', '').replace('_', '')
            if pattern_clean in col_lower or col_lower in pattern_clean:
                return col

    return None


def extract_username_from_email(email):
    """
    Extract username from email address.

    Example: "bk849@drexel.edu" -> "bk849"
    """
    if pd.isna(email) or not email or '@' not in str(email):
        return ''

    return str(email).split('@')[0].strip().lower()


def find_email_column(df):
    """
    Find the SCHOOL email column in a DataFrame.
    Only looks for "School email" specifically, not other email columns.
    """
    patterns = ['school email', 'schoolemail', 'school_email']

    for col in df.columns:
        col_lower = col.lower().replace(' ', '').replace('_', '').replace('-', '')
        for pattern in patterns:
            pattern_clean = pattern.replace(' ', '').replace('_', '').replace('-', '')
            if pattern_clean == col_lower:
                return col

    return None


def find_name_columns(df):
    """
    Find first-name and last-name columns in a DataFrame.

    Returns:
        (first_col, last_col) — either may be None if not found.
    """
    first_col = None
    last_col = None
    for col in df.columns:
        cl = col.lower()
        if 'first' in cl and 'name' in cl:
            first_col = col
        elif 'last' in cl and 'name' in cl:
            last_col = col
    return first_col, last_col


def build_student_score_maps(student_df, score_col):
    """
    Build username, student-ID, and name lookup maps from a student DataFrame.

    Args:
        student_df: DataFrame with student data
        score_col: Column name containing the score to map

    Returns:
        (username_map, id_map, name_map) where:
        - username_map: {username_str: score}
        - id_map:       {student_id_str: score}  (empty if no ID column found)
        - name_map:     {(last_lower, first_lower): score}  (empty if no name columns found)
    """
    username_map = {}
    id_map = {}
    name_map = {}

    email_col = find_email_column(student_df)
    id_col = find_student_id_column(student_df)
    first_col, last_col = find_name_columns(student_df)

    for _, row in student_df.iterrows():
        score = row.get(score_col)
        if pd.isna(score):
            continue

        if email_col:
            username = extract_username_from_email(row[email_col])
            if username:
                username_map[username] = score

        if id_col:
            student_id = normalize_student_id(row[id_col])
            if student_id:
                id_map[student_id] = score

        if first_col and last_col:
            first = str(row[first_col]).strip().lower() if pd.notna(row[first_col]) else ''
            last = str(row[last_col]).strip().lower() if pd.notna(row[last_col]) else ''
            if first and last:
                name_map[(last, first)] = score

    return username_map, id_map, name_map


def resolve_column(df, pattern):
    """
    Find the unique gradebook column whose name contains *pattern* as a substring.

    Args:
        df: Gradebook DataFrame
        pattern: Minimal substring identifying the target column

    Returns:
        Full column name string

    Raises:
        ValueError: If no column matches, or more than one column matches.
    """
    matches = [col for col in df.columns if pattern in col]
    if len(matches) == 0:
        raise ValueError(
            f"No gradebook column contains '{pattern}'.\n"
            f"Available columns: {', '.join(df.columns)}"
        )
    if len(matches) > 1:
        raise ValueError(
            f"Pattern '{pattern}' is ambiguous — matches {len(matches)} columns: "
            f"{', '.join(matches)}"
        )
    return matches[0]


def parse_assignment_filename(filename):
    """
    Parse assignment name from zyBooks filename.

    Examples:
        "DREXELENGR131Winter2026_Week_2_Challenge_Activities_report_102969_2026-02-01_104654.csv"
        -> ("Week 2 Challenge Activities", "W2 CA")

        "Week_1_Participation_Activities.csv"
        -> ("Week 1 Participation Activities", "W1 PA")

        "W1 PA_merged.csv"
        -> ("W1 PA", "W1 PA")

    Returns:
        Tuple of (full_name, abbreviated_name) or (None, None) if parsing fails
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

        full_name = f"Week {week_num} {assignment_type} {assignment_kind}"

        type_map = {
            'Participation Activities': 'PA',
            'Challenge Activities': 'CA',
            'In-Lab Labs': 'IL',
            'Out-of-Lab Labs': 'OL',
        }

        assignment_combo = f"{assignment_type} {assignment_kind}"
        abbrev_type = type_map.get(assignment_combo, None)

        if abbrev_type:
            abbrev_name = f"W{week_num} {abbrev_type}"
            return full_name, abbrev_name

    # Try simplified format: WX_YZ_grades.csv
    match = re.search(r'(W\d+)[_\s]+(PA|CA|IL|OL)', stem, re.IGNORECASE)
    if match:
        abbrev_name = f"{match.group(1).upper()} {match.group(2).upper()}"
        full_name = abbrev_name
        return full_name, abbrev_name

    return None, None
