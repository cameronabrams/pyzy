"""
Lab section assignment - reads per-section gradebooks and stamps each
student's lab section into the lecture gradebooks (in-place).

Lab section is parsed from the gradebook filename:
    lab60_gradebook.csv  ->  IL60
    sec_65_grades.csv    ->  IL65  (first 2+-digit number wins)
"""

import csv
import re
import sys
from pathlib import Path

import pandas as pd

from .common import (
    extract_username_from_email,
    find_email_column,
    find_name_columns,
    find_student_id_column,
    normalize_student_id,
    read_csv_with_trailing_comma_fix,
    resolve_column,
)
from .merge import find_username_column

LAB_SECTION_COL = 'Lab section'


def parse_lab_section(filename):
    """
    Extract a lab section label from the bare filename (not the full path).

    Examples:
        'lab60_gradebook.csv'   -> 'IL60'
        'sec_65_grades.csv'     -> 'IL65'

    Returns None if no two-or-more-digit number is found.
    """
    name = Path(filename).name
    m = re.search(r'(\d{2,})', name)
    return m.group(1) if m else None


def _build_lecture_index(lec_df):
    """Build {student_id: idx}, {username: idx}, {(last,first): idx} for a gradebook df."""
    id_col = find_student_id_column(lec_df)
    email_col = find_email_column(lec_df)
    un_col = find_username_column(lec_df)
    first_col, last_col = find_name_columns(lec_df)

    id_idx = {}
    username_idx = {}
    name_idx = {}

    for idx, row in lec_df.iterrows():
        if id_col:
            sid = normalize_student_id(row[id_col])
            if sid:
                id_idx[sid] = idx
        username = None
        if un_col and not pd.isna(row.get(un_col)):
            username = str(row[un_col]).strip().lower()
        elif email_col:
            username = extract_username_from_email(row[email_col])
        if username:
            username_idx[username] = idx
        if first_col and last_col:
            first = str(row[first_col]).strip().lower() if pd.notna(row[first_col]) else ''
            last = str(row[last_col]).strip().lower() if pd.notna(row[last_col]) else ''
            if first and last:
                name_idx[(last, first)] = idx

    return id_idx, username_idx, name_idx


def run_assign_lab_section(lecture_files, lab_files, quiet=False):
    """
    For each student in each lab-section gradebook, find them in the lecture
    gradebooks and stamp the Lab Section column.  Lecture gradebooks are
    updated in-place.
    """
    verbose = not quiet

    # Load lecture gradebooks
    lecture_dfs = {}
    lecture_paths = {}
    lecture_cols = {}  # resolved Lab Section column name per gradebook
    for lf in lecture_files:
        lp = Path(lf)
        if not lp.exists():
            print(f"ERROR: Lecture gradebook not found: {lf}")
            sys.exit(1)
        df = read_csv_with_trailing_comma_fix(lp)
        try:
            col = resolve_column(df, LAB_SECTION_COL)
        except ValueError:
            col = LAB_SECTION_COL
            df[col] = ''
        df[col] = df[col].astype(object)
        lecture_cols[lp.name] = col
        lecture_dfs[lp.name] = df
        lecture_paths[lp.name] = lp

    if not lecture_dfs:
        print("ERROR: No lecture gradebooks loaded.")
        sys.exit(1)

    # Build identity indices for every lecture gradebook
    lecture_indices = {
        name: _build_lecture_index(df)
        for name, df in lecture_dfs.items()
    }

    print("\nAssign Lab Sections")
    print("=" * 60)
    if verbose:
        for name, col in lecture_cols.items():
            print(f"   {name}: targeting column '{col}'")

    total_assigned = 0
    total_not_found = 0

    for lab_file in lab_files:
        lab_path = Path(lab_file)
        if not lab_path.exists():
            print(f"ERROR: Lab gradebook not found: {lab_file}")
            continue

        section = parse_lab_section(lab_path.name)
        if not section:
            print(f"WARNING: Cannot parse section number from '{lab_path.name}' — skipping")
            continue

        lab_df = read_csv_with_trailing_comma_fix(lab_path)

        lab_id_col = find_student_id_column(lab_df)
        lab_email_col = find_email_column(lab_df)
        lab_un_col = find_username_column(lab_df)
        lab_first_col, lab_last_col = find_name_columns(lab_df)

        if verbose:
            print(f"\n{section}  ({lab_path.name})  —  {len(lab_df)} student(s)")

        assigned = 0
        not_found_names = []

        for _, lab_row in lab_df.iterrows():
            lab_id = normalize_student_id(lab_row[lab_id_col]) if lab_id_col else None

            lab_username = None
            if lab_un_col and not pd.isna(lab_row.get(lab_un_col)):
                lab_username = str(lab_row[lab_un_col]).strip().lower()
            elif lab_email_col:
                lab_username = extract_username_from_email(lab_row[lab_email_col])

            lab_name = None
            if lab_first_col and lab_last_col:
                first = str(lab_row[lab_first_col]).strip().lower() if pd.notna(lab_row[lab_first_col]) else ''
                last = str(lab_row[lab_last_col]).strip().lower() if pd.notna(lab_row[lab_last_col]) else ''
                if first and last:
                    lab_name = (last, first)

            found = False
            for lec_name, lec_df in lecture_dfs.items():
                id_idx, username_idx, name_idx = lecture_indices[lec_name]

                lec_idx = None
                if lab_id and lab_id in id_idx:
                    lec_idx = id_idx[lab_id]
                elif lab_username and lab_username in username_idx:
                    lec_idx = username_idx[lab_username]
                elif lab_name and lab_name in name_idx:
                    lec_idx = name_idx[lab_name]

                if lec_idx is not None:
                    lec_df.at[lec_idx, lecture_cols[lec_name]] = section
                    found = True
                    assigned += 1
                    break

            if not found:
                display = (
                    f"{lab_row.get(lab_first_col, '')} {lab_row.get(lab_last_col, '')}".strip()
                    or lab_username
                    or lab_id
                    or '?'
                )
                not_found_names.append(display)

        if verbose:
            print(f"   Assigned: {assigned}  |  Not found: {len(not_found_names)}")
            for name in not_found_names:
                print(f"      NOT FOUND: {name}")

        total_assigned += assigned
        total_not_found += len(not_found_names)

    print(f"\nTotal assigned: {total_assigned}  |  Not found: {total_not_found}")

    print("\nWriting updated lecture gradebooks:")
    for lec_name, lec_df in lecture_dfs.items():
        lec_path = lecture_paths[lec_name]
        lec_df.to_csv(lec_path, index=False, encoding='utf-8-sig', quoting=csv.QUOTE_ALL)
        print(f"   {lec_path}")
