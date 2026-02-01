#!/usr/bin/env python3
"""
Grade Merger Script
Transfers grades from per-assignment CSVs to lecture section CSVs based on student ID matching.
Applies late penalties based on due date vs score date.

Usage:
    python merge_grades.py --lecture section_A.csv section_B.csv --assignment W1_PA.csv W1_CA.csv W2_PA.csv

Output:
    Creates *_merged.csv files for each lecture section with grades transferred from assignments.
"""

import argparse
import sys
import re
from pathlib import Path
from datetime import datetime
import pandas as pd


def parse_assignment_filename(filename):
    """
    Parse assignment name from zyBooks filename.
    
    Examples:
        "DREXELENGR131Winter2026_Week_2_Challenge_Activities_report_102969_2026-02-01_104654.csv"
        → ("Week 2 Challenge Activities", "W2 CA")
        
        "Week_1_Participation_Activities.csv" 
        → ("Week 1 Participation Activities", "W1 PA")
    
        "W1 PA_merged.csv"
        → ("W1 PA", "W1 PA")

    Args:
        filename: Filename to parse
        
    Returns:
        Tuple of (full_name, abbreviated_name) or (None, None) if parsing fails
    """
    stem = Path(filename).stem
    
    # Try zyBooks format: DREXEL...Winter2026_Week_X_Assignment_Type_...
    match = re.search(r'Week[_\s]+(\d+)[_\s]+(Participation|Challenge|In-Lab|Out-of-Lab)[_\s]+(Activities|Labs)', 
                     stem, re.IGNORECASE)
    
    if match:
        week_num = match.group(1)
        assignment_type = match.group(2).strip()
        assignment_kind = match.group(3).strip()
        
        # Construct full name
        full_name = f"Week {week_num} {assignment_type} {assignment_kind}"
        
        # Map to abbreviation
        type_map = {
            'Participation Activities': 'PA',
            'Challenge Activities': 'CA',
            'In-Lab Labs': 'IL',
            'Out-of-Lab Labs': 'OL'
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
        full_name = abbrev_name  # No expanded form available
        return full_name, abbrev_name
    
    return None, None


def read_csv_with_trailing_comma_fix(filepath):
    """
    Read a CSV file that may have trailing commas on each line.
    
    Args:
        filepath: Path to CSV file
        
    Returns:
        pandas DataFrame
    """
    import io
    
    # Read the file and fix trailing commas
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        lines = f.readlines()
    
    # Strip trailing commas
    fixed_lines = []
    for line in lines:
        # Remove trailing comma before newline
        line = line.rstrip('\n\r')
        if line.endswith(','):
            line = line[:-1]
        fixed_lines.append(line + '\n')
    
    # Read from the fixed data
    return pd.read_csv(io.StringIO(''.join(fixed_lines)))


def sort_assignment_columns(df):
    """
    Sort assignment columns by week number, then by assignment type.
    Order: W1 PA, W1 CA, W1 IL, W1 OL, W2 PA, W2 CA, W2 IL, W2 OL, etc.
    
    Args:
        df: DataFrame with assignment columns
        
    Returns:
        DataFrame with columns reordered
    """
    # Separate assignment columns from non-assignment columns
    assignment_cols = []
    non_assignment_cols = []
    
    assignment_type_order = {'PA': 0, 'CA': 1, 'IL': 2, 'OL': 3}
    
    for col in df.columns:
        # Check if it's an assignment column (W1 PA, W2 IL, etc.)
        match = re.match(r'W(\d+)\s+(PA|CA|IL|OL)', col.strip(), re.IGNORECASE)
        if match:
            week_num = int(match.group(1))
            assignment_type = match.group(2).upper()
            type_order = assignment_type_order.get(assignment_type, 99)
            assignment_cols.append((col, week_num, type_order))
        else:
            non_assignment_cols.append(col)
    
    # Sort assignments by week number, then by type order
    assignment_cols.sort(key=lambda x: (x[1], x[2]))
    sorted_assignment_names = [col[0] for col in assignment_cols]
    
    # Reorder: non-assignment columns first, then sorted assignments
    new_column_order = non_assignment_cols + sorted_assignment_names
    
    return df[new_column_order]


def normalize_student_id(student_id):
    """
    Normalize student ID to a string, handling float/int conversions.
    
    Examples:
        14788528.0 -> "14788528"
        14788528 -> "14788528"
        "14788528" -> "14788528"
    
    Args:
        student_id: Student ID value (could be float, int, or string)
        
    Returns:
        Normalized string representation of the ID
    """
    if pd.isna(student_id):
        return None
    
    # Convert to string
    id_str = str(student_id).strip()
    
    # If it looks like a float (has .0 at the end), remove it
    if '.' in id_str:
        try:
            # Try to convert to float then int to remove decimal
            id_float = float(id_str)
            if id_float == int(id_float):  # Check if it's a whole number
                return str(int(id_float))
        except (ValueError, OverflowError):
            pass
    
    return id_str


def find_student_id_column(df):
    """
    Find the student ID column in a DataFrame.
    
    Args:
        df: pandas DataFrame
        
    Returns:
        Column name containing student IDs, or None if not found
    """
    # Common patterns for student ID columns
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
    
    Example: "bk849@drexel.edu" → "bk849"
    
    Args:
        email: Email address
        
    Returns:
        Username portion before @, or empty string if invalid
    """
    if pd.isna(email) or not email or '@' not in str(email):
        return ''
    
    return str(email).split('@')[0].strip().lower()


def find_email_column(df):
    """
    Find the SCHOOL email column in a DataFrame.
    Only looks for "School email" specifically, not other email columns.
    
    Args:
        df: pandas DataFrame
        
    Returns:
        Column name containing school email addresses, or None if not found
    """
    # ONLY accept "School email" - ignore "Primary email" or other email columns
    patterns = ['school email', 'schoolemail', 'school_email']
    
    for col in df.columns:
        col_lower = col.lower().replace(' ', '').replace('_', '').replace('-', '')
        for pattern in patterns:
            pattern_clean = pattern.replace(' ', '').replace('_', '').replace('-', '')
            if pattern_clean == col_lower:
                return col
    
    return None


def find_username_column(df):
    """
    Find the username column in a DataFrame.
    
    Args:
        df: pandas DataFrame
        
    Returns:
        Column name containing usernames, or None if not found
    """
    patterns = ['username', 'user name', 'user_name']
    
    for col in df.columns:
        col_lower = col.lower().replace(' ', '').replace('_', '').replace('-', '')
        for pattern in patterns:
            pattern_clean = pattern.replace(' ', '').replace('_', '').replace('-', '')
            if pattern_clean == col_lower:
                return col
    
    return None


def merge_grades_from_assignments(lecture_files, assignment_files, 
                                  verbose=True):
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
        'failed_id_match_rows': pd.DataFrame()
    }
    
    # Load all lecture sections
    lecture_dfs = {}
    for filepath in lecture_files:
        lecture_name = Path(filepath).name
        # Handle BOM and trailing commas
        df = read_csv_with_trailing_comma_fix(filepath)
        
        lecture_dfs[lecture_name] = df
        
        if verbose:
            print(f"\n📚 Loaded lecture section: {lecture_name}")
            print(f"   Students: {len(df)}")
    
    # Process each assignment file
    for assignment_file in assignment_files:
        assignment_name = Path(assignment_file).name
        
        if verbose:
            print(f"\n📝 Processing assignment: {assignment_name}")
        
        # Parse assignment name from filename
        full_name, abbrev_name = parse_assignment_filename(assignment_name)
        
        if not abbrev_name:
            print(f"   ⚠️  Could not parse assignment name from filename, skipping")
            continue
        
        if verbose:
            print(f"   Assignment: {full_name} → {abbrev_name}")
        
        # Load assignment data - handle BOM and trailing commas
        assignment_df = read_csv_with_trailing_comma_fix(assignment_file)
        
        # Find required columns
        assign_id_col = find_student_id_column(assignment_df)
        assign_email_col = find_email_column(assignment_df)
        
        if verbose:
            print(f"   Assignment ID column: '{assign_id_col}'")
            print(f"   Assignment email column: '{assign_email_col}'")
        
        if not assign_id_col:
            print(f"   ❌ ERROR: Could not find Student ID column")
            continue
        
        # Find grade columns (look for "Percent score" or "Points earned")
        grade_col = None
        for col in assignment_df.columns:
            if 'percent score' in col.lower():
                grade_col = col
                break
        
        if not grade_col:
            print(f"   ⚠️  Could not find grade column (Percent score)")
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
        
        # Track all students in this assignment for orphan detection
        all_assignment_students = set()
        students_matched_this_assignment = set()
        
        # Process each lecture section
        for lecture_name, lecture_df in lecture_dfs.items():
            
            if verbose:
                print(f"\n   → Merging into: {lecture_name}")
            
            # Find ID/email columns in lecture
            lec_id_col = find_student_id_column(lecture_df)
            lec_email_col = find_email_column(lecture_df)
            lec_username_col = find_username_column(lecture_df)
            
            if verbose:
                print(f"      Lecture ID column: '{lec_id_col}'")
                print(f"      Lecture email column: '{lec_email_col}'")
                print(f"      Lecture username column: '{lec_username_col}'")
            
            if not lec_id_col and not lec_email_col and not lec_username_col:
                print(f"      ❌ ERROR: Could not find matching columns")
                continue
            
            # Build student map
            student_map = {}  # student_id -> row_index
            username_map = {}  # username -> row_index
            
            for idx, row in lecture_df.iterrows():
                # Map by student ID
                if lec_id_col:
                    student_id = normalize_student_id(row[lec_id_col])
                    if student_id:
                        student_map[student_id] = idx
                
                # Map by username (from email or username column)
                username = None
                if lec_username_col and not pd.isna(row[lec_username_col]):
                    username = str(row[lec_username_col]).strip().lower()
                elif lec_email_col:
                    username = extract_username_from_email(row[lec_email_col])
                
                if username:
                    username_map[username] = idx
            
            # Create assignment column if it doesn't exist
            if abbrev_name not in lecture_df.columns:
                lecture_df[abbrev_name] = ''
                if abbrev_name not in results['new_columns']:
                    results['new_columns'].append(abbrev_name)
                if verbose:
                    print(f"      ➕ Created new column: '{abbrev_name}'")
            
            grades_updated = 0
            matched_by_id = 0
            matched_by_email = 0
            failed_id_matches = []
            
            # Process each student in assignment file
            for _, assign_row in assignment_df.iterrows():
                # Get student ID
                assign_student_id = normalize_student_id(assign_row[assign_id_col])
                assign_username = None
                if assign_email_col:
                    assign_username = extract_username_from_email(assign_row[assign_email_col])
                
                # Track this student in the assignment
                student_key = assign_student_id or assign_username
                if student_key:
                    all_assignment_students.add(student_key)
                
                # Try to match student
                lecture_row_idx = None
                match_method = None
                
                # Try ID match first
                if assign_student_id and assign_student_id in student_map:
                    lecture_row_idx = student_map[assign_student_id]
                    match_method = 'id'
                    matched_by_id += 1
                
                # Try username match if ID failed
                elif assign_username and assign_username in username_map:
                    lecture_row_idx = username_map[assign_username]
                    match_method = 'email'
                    matched_by_email += 1
                    
                    # Verify ID matches (if both have IDs)
                    if assign_student_id and lec_id_col:
                        lec_student_id = normalize_student_id(lecture_df.at[lecture_row_idx, lec_id_col])
                        if lec_student_id and assign_student_id != lec_student_id:
                            failed_id_matches.append(assign_row.to_dict())
                            if verbose:
                                print(f"      ⚠️  ID mismatch: {assign_username} has ID {assign_student_id} "
                                      f"in assignment but {lec_student_id} in lecture")
                
                if lecture_row_idx is None:
                    # Student not found in this lecture section - they might be in another section
                    continue
                
                # Student matched in this lecture section!
                students_matched_this_assignment.add(student_key)
                results['matched_students'].add(student_key)
                if match_method == 'id':
                    results['match_methods']['by_id'] += 1
                else:
                    results['match_methods']['by_email'] += 1
                
                # Get base grade
                base_grade = assign_row[grade_col]
                if pd.isna(base_grade) or base_grade == '':
                    continue
                
                # Apply late penalty if date columns available
                final_grade = base_grade
                if due_date_col and score_date_col:
                    due_date = assign_row[due_date_col]
                    score_date = assign_row[score_date_col]
                    final_grade = calculate_late_penalty(
                        due_date, score_date, base_grade,
                        penalty_per_day, max_penalty
                    )
                    
                    if final_grade != base_grade:
                        results['late_penalties_applied'] += 1
                        if verbose:
                            print(f"      ⏰ Late penalty: {assign_username or assign_student_id} "
                                  f"{base_grade}% → {final_grade}%")
                
                # Update grade in lecture dataframe
                lecture_df.at[lecture_row_idx, abbrev_name] = final_grade
                grades_updated += 1
            
            # Store updated dataframe
            lecture_dfs[lecture_name] = lecture_df
            
            # Track statistics
            if lecture_name not in results['stats']:
                results['stats'][lecture_name] = {
                    'grades_updated': 0,
                    'matched_by_id': 0,
                    'matched_by_email': 0,
                    'columns_added': 0
                }
            
            results['stats'][lecture_name]['grades_updated'] += grades_updated
            results['stats'][lecture_name]['matched_by_id'] += matched_by_id
            results['stats'][lecture_name]['matched_by_email'] += matched_by_email
            
            if verbose:
                print(f"      Grades updated: {grades_updated}")
                print(f"      Matched: {matched_by_id} by ID, {matched_by_email} by email")
            
            # Collect failed ID matches for this lecture section
            if failed_id_matches:
                failed_df = pd.DataFrame(failed_id_matches)
                results['failed_id_match_rows'] = pd.concat([results['failed_id_match_rows'], failed_df],
                                                            ignore_index=True)
        
        # After processing all lecture sections, find truly orphaned students
        # (those in assignment but not matched in ANY lecture section)
        orphaned_students = all_assignment_students - students_matched_this_assignment
        
        if orphaned_students and verbose:
            print(f"\n   Students not found in any lecture section for this assignment: {len(orphaned_students)}")
        
        # Collect full rows for orphaned students
        for _, assign_row in assignment_df.iterrows():
            assign_student_id = normalize_student_id(assign_row[assign_id_col])
            assign_username = None
            if assign_email_col:
                assign_username = extract_username_from_email(assign_row[assign_email_col])
            
            student_key = assign_student_id or assign_username
            if student_key and student_key in orphaned_students:
                orphaned_df = pd.DataFrame([assign_row.to_dict()])
                results['orphaned_rows'] = pd.concat([results['orphaned_rows'], orphaned_df], 
                                                     ignore_index=True)
    
    # Sort columns in each lecture dataframe
    for lecture_name, df in lecture_dfs.items():
        lecture_dfs[lecture_name] = sort_assignment_columns(df)
    
    results['updated_dataframes'] = lecture_dfs
    
    return results


def main():
    parser = argparse.ArgumentParser(
        description='Merge grades from per-assignment files into lecture gradebooks',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
    python merge_grades.py --lecture section_A.csv section_B.csv \\
        --assignment W1_PA.csv W1_CA.csv W2_PA.csv W2_CA.csv
    
    python merge_grades.py -l sec_A.csv -a assignments/*.csv --output-dir merged/
        """
    )
    
    parser.add_argument(
        '--lecture', '-l',
        nargs='+',
        required=True,
        help='Lecture section CSV files'
    )
    
    parser.add_argument(
        '--assignment', '-a',
        nargs='+',
        help='Per-assignment CSV files from zyBooks'
    )
    
    parser.add_argument(
        '--assignment-dir', '-ad',
        help='Directory containing per-assignment CSV files (alternative to --assignment)'
    )

    parser.add_argument(
        '--assignment-pattern', '-ap',
        default='*_bblearn.csv',
        help='Filename pattern to match assignment CSVs in assignment directory (default: "*_bblearn.csv")'
    )

    parser.add_argument(
        '--output-dir', '-o',
        default='.',
        help='Output directory for merged files (default: current directory)'
    )
    
    parser.add_argument(
        '--quiet', '-q',
        action='store_true',
        help='Suppress verbose output'
    )
    
    args = parser.parse_args()
    
    # Verify lecture files exist
    for filepath in args.lecture:
        if not Path(filepath).exists():
            print(f"❌ ERROR: File not found: {filepath}")
            sys.exit(1)
    
    # Verify assignment files exist
    if args.assignment:
        for filepath in args.assignment:
            if not Path(filepath).exists():
                print(f"❌ ERROR: File not found: {filepath}")
                sys.exit(1)
    elif args.assignment_dir:
        assignment_dir = Path(args.assignment_dir)
        if not assignment_dir.exists() or not assignment_dir.is_dir():
            print(f"❌ ERROR: Assignment directory not found: {assignment_dir}")
            sys.exit(1)
        
        # Find all matching files in the directory
        args.assignment = [str(p) for p in assignment_dir.glob(args.assignment_pattern)]
        if not args.assignment:
            print(f"❌ ERROR: No assignment files found in {assignment_dir} matching pattern '{args.assignment_pattern}'")
            sys.exit(1)
    
    # Create output directory if needed
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Merge grades
    print("\n🎓 Grade Merger Script")
    print("=" * 60)
    
    results = merge_grades_from_assignments(
        lecture_files=args.lecture,
        assignment_files=args.assignment,
        verbose=not args.quiet
    )
    
    # Print summary
    print("\n" + "=" * 60)
    print("📊 MERGE SUMMARY")
    print("=" * 60)
    
    print(f"\n✓ Successfully matched {len(results['matched_students'])} unique students")
    print(f"   Matched by Student ID: {results['match_methods']['by_id']}")
    print(f"   Matched by Email: {results['match_methods']['by_email']}")
    
    if results['new_columns']:
        print(f"\n➕ New columns created: {', '.join(results['new_columns'])}")
    
    # Write output files
    print("\n💾 Writing output files:")
    for lecture_name, df in results['updated_dataframes'].items():
        # Drop any Unnamed columns that may have snuck in
        unnamed_cols = [col for col in df.columns if 'Unnamed' in str(col)]
        if unnamed_cols:
            df = df.drop(columns=unnamed_cols)
        
        output_name = lecture_name.replace('.csv', '_merged.csv')
        output_path = output_dir / output_name
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"   ✓ {output_path}")
        stats = results['stats'].get(lecture_name, {})
        print(f"      Grades updated: {stats.get('grades_updated', 0)}")
        print(f"      Matched: {stats.get('matched_by_id', 0)} by ID, {stats.get('matched_by_email', 0)} by email")
    
    # Write orphaned students file if any
    if len(results['orphaned_rows']) > 0:
        orphaned_path = output_dir / 'orphaned_students.csv'
        results['orphaned_rows'].to_csv(orphaned_path, index=False, encoding='utf-8-sig')
        print(f"\n   ⚠️  {orphaned_path}")
        print(f"      {len(results['orphaned_rows'])} students not found in any lecture section")
    
    # Write failed ID match report if any
    if len(results['failed_id_match_rows']) > 0:
        failed_id_path = output_dir / 'failed_id_matches.csv'
        results['failed_id_match_rows'].to_csv(failed_id_path, index=False, encoding='utf-8-sig')
        print(f"\n   ⚠️  {failed_id_path}")
        print(f"      {len(results['failed_id_match_rows'])} students matched by username but ID mismatch")
    
    print("\n✅ Done!")


if __name__ == '__main__':
    main()