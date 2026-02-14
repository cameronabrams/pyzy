"""
Grade Merger - transfers grades from per-assignment CSVs to lecture section CSVs
based on student ID matching.
"""

import re
from pathlib import Path

import pandas as pd

from .common import (
    find_email_column,
    find_student_id_column,
    extract_username_from_email,
    normalize_student_id,
    parse_assignment_filename,
    read_csv_with_trailing_comma_fix,
)


def find_username_column(df):
    """Find the username column in a DataFrame."""
    patterns = ['username', 'user name', 'user_name']

    for col in df.columns:
        col_lower = col.lower().replace(' ', '').replace('_', '').replace('-', '')
        for pattern in patterns:
            pattern_clean = pattern.replace(' ', '').replace('_', '').replace('-', '')
            if pattern_clean == col_lower:
                return col

    return None


def sort_assignment_columns(df):
    """
    Sort assignment columns by week number, then by assignment type.
    Order: W1 PA, W1 CA, W1 IL, W1 OL, W2 PA, W2 CA, W2 IL, W2 OL, etc.
    """
    assignment_cols = []
    non_assignment_cols = []

    assignment_type_order = {'PA': 0, 'CA': 1, 'IL': 2, 'OL': 3}

    for col in df.columns:
        match = re.match(r'W(\d+)\s+(PA|CA|IL|OL)', col.strip(), re.IGNORECASE)
        if match:
            week_num = int(match.group(1))
            assignment_type = match.group(2).upper()
            type_order = assignment_type_order.get(assignment_type, 99)
            assignment_cols.append((col, week_num, type_order))
        else:
            non_assignment_cols.append(col)

    assignment_cols.sort(key=lambda x: (x[1], x[2]))
    sorted_assignment_names = [col[0] for col in assignment_cols]

    new_column_order = non_assignment_cols + sorted_assignment_names
    return df[new_column_order]


def merge_grades_from_assignments(lecture_files, assignment_files, verbose=True):
    """
    Merge grades from per-assignment files into lecture section files.

    Args:
        lecture_files: List of lecture section CSV file paths
        assignment_files: List of per-assignment CSV file paths
        verbose: Print detailed progress information

    Returns:
        Dictionary with merge results and statistics
    """
    results = {
        'updated_dataframes': {},
        'stats': {},
        'matched_students': set(),
        'students_not_found': set(),
        'new_columns': [],
        'match_methods': {'by_id': 0, 'by_email': 0},
        'orphaned_rows': pd.DataFrame(),
        'failed_id_match_rows': pd.DataFrame(),
    }

    # Load all lecture sections
    lecture_dfs = {}
    for filepath in lecture_files:
        lecture_name = Path(filepath).name
        df = read_csv_with_trailing_comma_fix(filepath)
        lecture_dfs[lecture_name] = df

        if verbose:
            print(f"\nLoaded lecture section: {lecture_name}")
            print(f"   Students: {len(df)}")

    # Process each assignment file
    for assignment_file in assignment_files:
        assignment_name = Path(assignment_file).name

        if verbose:
            print(f"\nProcessing assignment: {assignment_name}")

        full_name, abbrev_name = parse_assignment_filename(assignment_name)

        if not abbrev_name:
            print(f"   WARNING: Could not parse assignment name from filename, skipping")
            continue

        if verbose:
            print(f"   Assignment: {full_name} -> {abbrev_name}")

        assignment_df = read_csv_with_trailing_comma_fix(assignment_file)

        assign_id_col = find_student_id_column(assignment_df)
        assign_email_col = find_email_column(assignment_df)

        if verbose:
            print(f"   Assignment ID column: '{assign_id_col}'")
            print(f"   Assignment email column: '{assign_email_col}'")

        if not assign_id_col:
            print(f"   ERROR: Could not find Student ID column")
            continue

        # Find grade column
        grade_col = None
        for col in assignment_df.columns:
            if 'percent score' in col.lower():
                grade_col = col
                break

        if not grade_col:
            print(f"   WARNING: Could not find grade column (Percent score)")
            continue

        # Find date columns
        due_date_col = None
        score_date_col = None
        for col in assignment_df.columns:
            if 'due date' in col.lower():
                due_date_col = col
            elif 'score date' in col.lower():
                score_date_col = col

        if verbose:
            print(f"   Grade column: '{grade_col}'")
            print(f"   Due date column: '{due_date_col}'")
            print(f"   Score date column: '{score_date_col}'")

        all_assignment_students = set()
        students_matched_this_assignment = set()

        # Process each lecture section
        for lecture_name, lecture_df in lecture_dfs.items():
            if verbose:
                print(f"\n   -> Merging into: {lecture_name}")

            lec_id_col = find_student_id_column(lecture_df)
            lec_email_col = find_email_column(lecture_df)
            lec_username_col = find_username_column(lecture_df)

            if verbose:
                print(f"      Lecture ID column: '{lec_id_col}'")
                print(f"      Lecture email column: '{lec_email_col}'")
                print(f"      Lecture username column: '{lec_username_col}'")

            if not lec_id_col and not lec_email_col and not lec_username_col:
                print(f"      ERROR: Could not find matching columns")
                continue

            # Build student map
            student_map = {}
            username_map = {}

            for idx, row in lecture_df.iterrows():
                if lec_id_col:
                    student_id = normalize_student_id(row[lec_id_col])
                    if student_id:
                        student_map[student_id] = idx

                username = None
                if lec_username_col and not pd.isna(row[lec_username_col]):
                    username = str(row[lec_username_col]).strip().lower()
                elif lec_email_col:
                    username = extract_username_from_email(row[lec_email_col])

                if username:
                    username_map[username] = idx

            if abbrev_name not in lecture_df.columns:
                lecture_df[abbrev_name] = ''
                if abbrev_name not in results['new_columns']:
                    results['new_columns'].append(abbrev_name)
                if verbose:
                    print(f"      Created new column: '{abbrev_name}'")

            grades_updated = 0
            matched_by_id = 0
            matched_by_email = 0
            failed_id_matches = []

            for _, assign_row in assignment_df.iterrows():
                assign_student_id = normalize_student_id(assign_row[assign_id_col])
                assign_username = None
                if assign_email_col:
                    assign_username = extract_username_from_email(assign_row[assign_email_col])

                student_key = assign_student_id or assign_username
                if student_key:
                    all_assignment_students.add(student_key)

                lecture_row_idx = None
                match_method = None

                if assign_student_id and assign_student_id in student_map:
                    lecture_row_idx = student_map[assign_student_id]
                    match_method = 'id'
                    matched_by_id += 1
                elif assign_username and assign_username in username_map:
                    lecture_row_idx = username_map[assign_username]
                    match_method = 'email'
                    matched_by_email += 1

                    if assign_student_id and lec_id_col:
                        lec_student_id = normalize_student_id(
                            lecture_df.at[lecture_row_idx, lec_id_col]
                        )
                        if lec_student_id and assign_student_id != lec_student_id:
                            failed_id_matches.append(assign_row.to_dict())
                            if verbose:
                                print(
                                    f"      WARNING: ID mismatch: {assign_username} has ID "
                                    f"{assign_student_id} in assignment but {lec_student_id} in lecture"
                                )

                if lecture_row_idx is None:
                    continue

                students_matched_this_assignment.add(student_key)
                results['matched_students'].add(student_key)
                if match_method == 'id':
                    results['match_methods']['by_id'] += 1
                else:
                    results['match_methods']['by_email'] += 1

                base_grade = assign_row[grade_col]
                if pd.isna(base_grade) or base_grade == '':
                    continue

                final_grade = base_grade

                lecture_df.at[lecture_row_idx, abbrev_name] = final_grade
                grades_updated += 1

            lecture_dfs[lecture_name] = lecture_df

            if lecture_name not in results['stats']:
                results['stats'][lecture_name] = {
                    'grades_updated': 0,
                    'matched_by_id': 0,
                    'matched_by_email': 0,
                    'columns_added': 0,
                }

            results['stats'][lecture_name]['grades_updated'] += grades_updated
            results['stats'][lecture_name]['matched_by_id'] += matched_by_id
            results['stats'][lecture_name]['matched_by_email'] += matched_by_email

            if verbose:
                print(f"      Grades updated: {grades_updated}")
                print(f"      Matched: {matched_by_id} by ID, {matched_by_email} by email")

            if failed_id_matches:
                failed_df = pd.DataFrame(failed_id_matches)
                results['failed_id_match_rows'] = pd.concat(
                    [results['failed_id_match_rows'], failed_df], ignore_index=True
                )

        orphaned_students = all_assignment_students - students_matched_this_assignment

        if orphaned_students and verbose:
            print(
                f"\n   Students not found in any lecture section for this assignment: "
                f"{len(orphaned_students)}"
            )

        for _, assign_row in assignment_df.iterrows():
            assign_student_id = normalize_student_id(assign_row[assign_id_col])
            assign_username = None
            if assign_email_col:
                assign_username = extract_username_from_email(assign_row[assign_email_col])

            student_key = assign_student_id or assign_username
            if student_key and student_key in orphaned_students:
                orphaned_df = pd.DataFrame([assign_row.to_dict()])
                results['orphaned_rows'] = pd.concat(
                    [results['orphaned_rows'], orphaned_df], ignore_index=True
                )

    for lecture_name, df in lecture_dfs.items():
        lecture_dfs[lecture_name] = sort_assignment_columns(df)

    results['updated_dataframes'] = lecture_dfs
    return results


def run_merge(lecture_files, assignment_files, assignment_dir=None,
              assignment_pattern='*_bblearn.csv', output_dir='.', quiet=False):
    """
    Run the merge workflow.

    Args:
        lecture_files: List of lecture section CSV paths
        assignment_files: List of assignment CSV paths (or None if using assignment_dir)
        assignment_dir: Directory containing assignment CSVs (alternative to assignment_files)
        assignment_pattern: Glob pattern for assignment files in directory
        output_dir: Output directory for merged files
        quiet: Suppress verbose output
    """
    import sys

    # Verify lecture files exist
    for filepath in lecture_files:
        if not Path(filepath).exists():
            print(f"ERROR: File not found: {filepath}")
            sys.exit(1)

    # Resolve assignment files
    if assignment_files:
        for filepath in assignment_files:
            if not Path(filepath).exists():
                print(f"ERROR: File not found: {filepath}")
                sys.exit(1)
    elif assignment_dir:
        adir = Path(assignment_dir)
        if not adir.exists() or not adir.is_dir():
            print(f"ERROR: Assignment directory not found: {adir}")
            sys.exit(1)
        assignment_files = [str(p) for p in adir.glob(assignment_pattern)]
        if not assignment_files:
            print(
                f"ERROR: No assignment files found in {adir} matching "
                f"pattern '{assignment_pattern}'"
            )
            sys.exit(1)
    else:
        print("ERROR: Must provide --assignment or --assignment-dir")
        sys.exit(1)

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    print("\nGrade Merger")
    print("=" * 60)

    results = merge_grades_from_assignments(
        lecture_files=lecture_files,
        assignment_files=assignment_files,
        verbose=not quiet,
    )

    # Print summary
    print("\n" + "=" * 60)
    print("MERGE SUMMARY")
    print("=" * 60)

    print(f"\nSuccessfully matched {len(results['matched_students'])} unique students")
    print(f"   Matched by Student ID: {results['match_methods']['by_id']}")
    print(f"   Matched by Email: {results['match_methods']['by_email']}")

    if results['new_columns']:
        print(f"\nNew columns created: {', '.join(results['new_columns'])}")

    # Write output files
    print("\nWriting output files:")
    for lecture_name, df in results['updated_dataframes'].items():
        unnamed_cols = [col for col in df.columns if 'Unnamed' in str(col)]
        if unnamed_cols:
            df = df.drop(columns=unnamed_cols)

        output_name = lecture_name.replace('.csv', '_merged.csv')
        output_path = out / output_name
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"   {output_path}")
        stats = results['stats'].get(lecture_name, {})
        print(f"      Grades updated: {stats.get('grades_updated', 0)}")
        print(
            f"      Matched: {stats.get('matched_by_id', 0)} by ID, "
            f"{stats.get('matched_by_email', 0)} by email"
        )

    if len(results['orphaned_rows']) > 0:
        orphaned_path = out / 'orphaned_students.csv'
        results['orphaned_rows'].to_csv(orphaned_path, index=False, encoding='utf-8-sig')
        print(f"\n   WARNING: {orphaned_path}")
        print(f"      {len(results['orphaned_rows'])} students not found in any lecture section")

    if len(results['failed_id_match_rows']) > 0:
        failed_id_path = out / 'failed_id_matches.csv'
        results['failed_id_match_rows'].to_csv(failed_id_path, index=False, encoding='utf-8-sig')
        print(f"\n   WARNING: {failed_id_path}")
        print(
            f"      {len(results['failed_id_match_rows'])} students matched by username "
            f"but ID mismatch"
        )

    print("\nDone!")
