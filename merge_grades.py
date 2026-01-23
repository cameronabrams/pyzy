#!/usr/bin/env python3
"""
Grade Merger Script
Transfers grades from lab section CSVs to lecture section CSVs based on student ID matching.

Usage:
    python merge_grades.py --lecture section_A.csv section_B.csv --lab section_60.csv section_61.csv

Output:
    Creates *_merged.csv files for each lecture section with grades transferred from labs.
"""

import argparse
import sys
import re
from pathlib import Path
import pandas as pd


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


def clean_assignment_name(col_name):
    """
    Clean assignment column name by removing metadata.
    
    Example: "W1 PA [Total Pts: 0 Text] |3743530" -> "W1 PA"
    
    Args:
        col_name: Original column name
        
    Returns:
        Cleaned column name with metadata removed
    """
    # Find the '[' character and take everything before it
    if '[' in col_name:
        col_name = col_name.split('[')[0]
    
    # Right strip whitespace
    return col_name.rstrip()


def translate_assignment_name(col_name):
    """
    Translate full assignment names to abbreviated form.
    
    Examples:
        "Week 1 Participation Activities (165)" → "W1 PA"
        "Week 2 In-Lab Labs (30.0)" → "W2 IL"
        "Week 3 Challenge Activities (46)" → "W3 CA"
        "Week 4 Out-of-Lab Labs (25.0)" → "W4 OL"
    
    Args:
        col_name: Full assignment column name
        
    Returns:
        Abbreviated assignment name, or original if no pattern matches
    """
    # Extract week number and assignment type (remove point values in parentheses)
    match = re.match(r'Week\s+(\d+)\s+(.+?)(?:\s*\([\d.]+\))?$', col_name.strip())
    
    if not match:
        return col_name
    
    week_num = match.group(1)
    assignment_type = match.group(2).strip()
    
    # Mapping of full names to abbreviations
    type_map = {
        'Participation Activities': 'PA',
        'In-Lab Labs': 'IL',
        'Challenge Activities': 'CA',
        'Out-of-Lab Labs': 'OL'
    }
    
    # Find matching type
    for full_name, abbrev in type_map.items():
        if full_name.lower() == assignment_type.lower():
            return f"W{week_num} {abbrev}"
    
    # If no match, return original
    return col_name


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


def is_gradeable_assignment(col_name):
    """
    Check if a column represents a gradeable assignment.
    Only returns True for columns matching the Week N pattern.
    
    Args:
        col_name: Column name to check
        
    Returns:
        True if this is a gradeable assignment column (Week N pattern), False otherwise
    """
    # Only accept columns that match "Week N ..." pattern
    if re.match(r'Week\s+\d+\s+', col_name.strip(), re.IGNORECASE):
        return True
    
    # Also accept already-abbreviated forms like "W1 PA", "W2 IL", etc.
    if re.match(r'W\d+\s+(PA|IL|CA|OL)', col_name.strip(), re.IGNORECASE):
        return True
    
    return False


def normalize_column_name(name):
    """Normalize column name for fuzzy matching."""
    return name.lower().replace('_', ' ').replace('-', ' ').strip()


def find_matching_column(lab_col, lecture_cols):
    """
    Find matching column in lecture gradebook for a lab column.
    Both lab and lecture columns are cleaned before comparison.
    
    Args:
        lab_col: Column name from lab gradebook (should already be cleaned)
        lecture_cols: List of column names from lecture gradebook
        
    Returns:
        Matching column name from lecture_cols, or None
    """
    # Clean and normalize the lab column
    cleaned_lab = clean_assignment_name(lab_col)
    normalized_lab = normalize_column_name(cleaned_lab)
    
    # Exact match first (comparing cleaned versions)
    for lec_col in lecture_cols:
        cleaned_lec = clean_assignment_name(lec_col)
        if normalize_column_name(cleaned_lec) == normalized_lab:
            return lec_col
    
    # Fuzzy match - contains (comparing cleaned versions)
    for lec_col in lecture_cols:
        cleaned_lec = clean_assignment_name(lec_col)
        normalized_lec = normalize_column_name(cleaned_lec)
        if normalized_lab in normalized_lec or normalized_lec in normalized_lab:
            return lec_col
    
    return None


def load_csv_robust(filepath):
    """
    Load CSV with robust handling of various formats.
    
    Args:
        filepath: Path to CSV file
        
    Returns:
        pandas DataFrame with stripped whitespace and no Unnamed columns
    """
    # Try different encodings, with utf-8-sig first to handle BOM
    encodings = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']
    
    for encoding in encodings:
        try:
            # CRITICAL: index_col=False prevents pandas from treating first column as index
            # on_bad_lines='warn' handles trailing commas gracefully
            try:
                df = pd.read_csv(filepath, encoding=encoding, index_col=False, 
                                skipinitialspace=True, on_bad_lines='warn')
            except TypeError:
                # Fallback for older pandas versions
                df = pd.read_csv(filepath, encoding=encoding, index_col=False,
                                skipinitialspace=True, error_bad_lines=False, warn_bad_lines=True)
            
            # Strip whitespace from column names
            df.columns = df.columns.str.strip()
            
            # Drop any "Unnamed" columns that may have been created by trailing commas
            unnamed_cols = [col for col in df.columns if 'Unnamed' in str(col)]
            if unnamed_cols:
                df = df.drop(columns=unnamed_cols)
            
            # Strip whitespace from string values
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].str.strip()
            
            return df
        except (UnicodeDecodeError, Exception) as e:
            continue
    
    raise ValueError(f"Could not read {filepath} with any standard encoding")


def merge_grades_from_master(lecture_files, master_file, verbose=True):
    """
    Merge grades from master gradebook into lecture gradebooks.
    Uses both Student ID and email username for robust student matching.
    Translates full assignment names to abbreviated form.
    
    Args:
        lecture_files: List of paths to lecture CSV files
        master_file: Path to master gradebook CSV file
        verbose: Print diagnostic information
        
    Returns:
        Dictionary mapping lecture filenames to updated DataFrames
    """
    results = {
        'updated_dataframes': {},
        'stats': {},
        'students_not_found': [],
        'orphaned_rows': [],  # Full rows from master for unmatched students
        'failed_id_match_rows': [],  # Students who matched by username but ID failed
        'new_columns': [],
        'matched_students': set(),
        'match_methods': {'by_id': 0, 'by_email': 0}
    }
    
    # Track all students seen in master
    all_master_students = set()
    
    # Load master gradebook
    if verbose:
        print(f"\n📊 Loading master gradebook: {Path(master_file).name}")
    
    master_df = load_csv_robust(master_file)
    
    if verbose:
        print(f"   Rows: {len(master_df)}, Columns: {len(master_df.columns)}")
    
    # Find master student ID and email columns
    master_id_col = find_student_id_column(master_df)
    master_email_col = find_email_column(master_df)
    
    if not master_id_col and not master_email_col:
        raise ValueError("Could not find Student ID or Email column in master gradebook")
    
    if verbose:
        if master_id_col:
            print(f"   ✓ Master Student ID column: '{master_id_col}'")
            sample_master_ids = master_df[master_id_col].dropna().head(3).tolist()
            print(f"   Sample master IDs: {sample_master_ids}")
        if master_email_col:
            print(f"   ✓ Master Email column: '{master_email_col}'")
            sample_emails = master_df[master_email_col].dropna().head(3).tolist()
            sample_usernames = [extract_username_from_email(e) for e in sample_emails]
            print(f"   Sample master emails: {sample_emails}")
            print(f"   Sample master usernames: {sample_usernames}")
    
    # Track which master rows were matched (by index)
    matched_master_indices = set()
    # Track failed ID matches with details: {master_idx: (master_id, matched_lecture_id, master_email, master_name)}
    failed_id_match_details = {}
    
    # Load lecture gradebooks
    lecture_data = {}
    for filepath in lecture_files:
        df = load_csv_robust(filepath)
        lecture_data[Path(filepath).name] = df
        
        if verbose:
            print(f"\n📘 Loaded lecture file: {Path(filepath).name}")
            print(f"   Rows: {len(df)}, Columns: {len(df.columns)}")
    
    # Process each lecture section
    for lecture_name, lecture_df in lecture_data.items():
        if verbose:
            print(f"\n{'='*60}")
            print(f"Processing lecture section: {lecture_name}")
            print('='*60)
        
        # Find lecture student ID and username columns
        lecture_id_col = find_student_id_column(lecture_df)
        lecture_username_col = find_username_column(lecture_df)
        
        if not lecture_id_col and not lecture_username_col:
            print(f"❌ ERROR: Could not find Student ID or Username column in {lecture_name}")
            print(f"   Available columns: {', '.join(lecture_df.columns)}")
            continue
        
        if verbose:
            print(f"   Available columns: {', '.join(lecture_df.columns[:10])}" + ("..." if len(lecture_df.columns) > 10 else ""))
            if lecture_id_col:
                print(f"✓ Lecture Student ID column: '{lecture_id_col}'")
            else:
                print(f"⚠️  No Student ID column found in lecture")
            if lecture_username_col:
                print(f"✓ Lecture Username column: '{lecture_username_col}'")
            else:
                print(f"⚠️  No Username column found in lecture")
        
        # Build lookup maps for lecture students (by both ID and username)
        student_id_map = {}  # student_id -> row_index
        student_username_map = {}  # username -> row_index
        
        for idx, row in lecture_df.iterrows():
            # Map by student ID
            if lecture_id_col:
                student_id = row[lecture_id_col]
                student_id_str = normalize_student_id(student_id)
                if student_id_str:
                    student_id_map[student_id_str] = idx
            
            # Map by username (stored directly in lecture gradebook)
            if lecture_username_col:
                username = row[lecture_username_col]
                if pd.notna(username):
                    username_str = str(username).strip().lower()
                    student_username_map[username_str] = idx
        
        if verbose:
            print(f"   Students in lecture: {len(student_id_map)} by ID, {len(student_username_map)} by username")
            if student_id_map:
                sample_ids = list(student_id_map.keys())[:3]
                print(f"   Sample lecture IDs: {sample_ids}")
            if student_username_map:
                sample_usernames = list(student_username_map.keys())[:3]
                print(f"   Sample lecture usernames: {sample_usernames}")
            
            # Show raw data from first row for debugging
            if len(lecture_df) > 0:
                print(f"\n   🔍 First lecture row raw data:")
                first_row = lecture_df.iloc[0]
                print(f"      All columns and values:")
                for col in lecture_df.columns:
                    print(f"        '{col}': '{first_row[col]}'")
                if lecture_id_col:
                    print(f"\n      Detected ID column '{lecture_id_col}' contains: '{first_row[lecture_id_col]}'")
                if lecture_username_col:
                    print(f"      Detected Username column '{lecture_username_col}' contains: '{first_row[lecture_username_col]}'")
        
        # Initialize stats for this section
        grades_updated = 0
        columns_added = 0
        matched_by_id = 0
        matched_by_email = 0
        
        # Debug: track first few match attempts
        debug_count = 0
        
        # Process each student in master gradebook
        for master_idx, master_row in master_df.iterrows():
            # Get student identifiers from master
            master_student_id = None
            if master_id_col:
                master_student_id = normalize_student_id(master_row[master_id_col])
            
            master_email = None
            master_username = None
            if master_email_col:
                master_email = master_row[master_email_col]
                master_username = extract_username_from_email(master_email)
            
            # Track that we've seen this student
            identifier = master_student_id or master_username or "unknown"
            all_master_students.add(identifier)
            
            # Debug output for first 3 students
            if verbose and debug_count < 3:
                print(f"\n  🔍 Debug - Attempting to match student:")
                print(f"     Master ID: {master_student_id}")
                print(f"     Master email: {master_email}")
                print(f"     Master username (from email): {master_username}")
                print(f"     ID in lecture map? {master_student_id in student_id_map if master_student_id else 'N/A'}")
                print(f"     Username in lecture map? {master_username in student_username_map if master_username else 'N/A'}")
                debug_count += 1
            
            # Try to find student in lecture section
            # SEQUENTIAL MATCHING: Try Student ID first, ONLY if that fails try username
            lecture_row_idx = None
            match_method = None
            id_match_failed = False
            
            # Step 1: Try matching by student ID
            if master_student_id and master_student_id in student_id_map:
                lecture_row_idx = student_id_map[master_student_id]
                match_method = 'id'
                matched_by_id += 1
            # Step 2: Only if Student ID didn't match, try matching by username
            elif master_username and master_username in student_username_map:
                lecture_row_idx = student_username_map[master_username]
                match_method = 'email'
                matched_by_email += 1
                # Check if student HAD an ID but it didn't match
                if master_student_id:
                    # Get the correct ID from the matched lecture row
                    matched_lecture_id = lecture_df.at[lecture_row_idx, lecture_id_col] if lecture_id_col else None
                    matched_lecture_id = normalize_student_id(matched_lecture_id) if matched_lecture_id else "N/A"
                    
                    # Get student name from master
                    last_name = ""
                    first_name = ""
                    for col in master_df.columns:
                        if 'last' in col.lower() and 'name' in col.lower():
                            last_name = master_row[col] if pd.notna(master_row[col]) else ""
                        if 'first' in col.lower() and 'name' in col.lower():
                            first_name = master_row[col] if pd.notna(master_row[col]) else ""
                    master_name = f"{last_name}, {first_name}".strip(", ")
                    
                    # Store the details
                    failed_id_match_details[master_idx] = {
                        'Student Name': master_name,
                        'Email': master_email,
                        'Apparent ID (Master)': master_student_id,
                        'Matched ID (Lecture)': matched_lecture_id
                    }
            
            if lecture_row_idx is None:
                # Student not found in this lecture section by either method
                continue
            
            # Student found! Mark this master row as matched
            matched_master_indices.add(master_idx)
            results['matched_students'].add(identifier)
            if match_method == 'id':
                results['match_methods']['by_id'] += 1
            else:
                results['match_methods']['by_email'] += 1
            
            # Transfer grades for ONLY assignment columns
            for master_col in master_df.columns:
                # Skip identification columns
                if (master_col == master_id_col or 
                    master_col == master_email_col or
                    'name' in master_col.lower() or
                    'email' in master_col.lower() or
                    'section' in master_col.lower() or
                    'class' in master_col.lower()):
                    continue
                
                # ONLY process assignment columns (Week N pattern)
                if not is_gradeable_assignment(master_col):
                    continue
                
                grade = master_row[master_col]
                if pd.isna(grade) or grade == '':
                    continue
                
                # Translate assignment name (e.g., "Week 1 Participation Activities" -> "W1 PA")
                abbreviated_col = translate_assignment_name(master_col)
                
                # Find or create column in lecture data
                lecture_col = find_matching_column(abbreviated_col, lecture_df.columns)
                
                if lecture_col is None:
                    # Create new column with abbreviated name
                    lecture_col = abbreviated_col
                    lecture_df[lecture_col] = ''
                    columns_added += 1
                    if lecture_col not in results['new_columns']:
                        results['new_columns'].append(lecture_col)
                    if verbose:
                        print(f"     ➕ Created new column: '{lecture_col}' (from '{master_col}')")
                
                # Update the grade
                lecture_df.at[lecture_row_idx, lecture_col] = grade
                grades_updated += 1
        
        # Store results with sorted columns
        results['updated_dataframes'][lecture_name] = sort_assignment_columns(lecture_df)
        results['stats'][lecture_name] = {
            'grades_updated': grades_updated,
            'columns_added': columns_added,
            'matched_by_id': matched_by_id,
            'matched_by_email': matched_by_email
        }
        
        if verbose:
            print(f"\n  Summary for {lecture_name}:")
            print(f"    Grades updated: {grades_updated}")
            print(f"    Columns added: {columns_added}")
            print(f"    Matched by ID: {matched_by_id}")
            print(f"    Matched by email: {matched_by_email}")
    
    # Now determine which students were truly not found in ANY lecture section
    results['students_not_found'] = sorted(list(all_master_students - results['matched_students']))
    
    # Collect full rows for orphaned students
    orphaned_mask = ~master_df.index.isin(matched_master_indices)
    results['orphaned_rows'] = master_df[orphaned_mask].copy()
    
    # Create focused failed ID matches report
    if failed_id_match_details:
        results['failed_id_match_rows'] = pd.DataFrame.from_dict(
            failed_id_match_details, 
            orient='index'
        ).reset_index(drop=True)
    else:
        results['failed_id_match_rows'] = pd.DataFrame()
    
    if verbose and len(results['orphaned_rows']) > 0:
        print(f"\n⚠️  Found {len(results['orphaned_rows'])} orphaned students in master gradebook")
    
    if verbose and len(results['failed_id_match_rows']) > 0:
        print(f"\n⚠️  Found {len(results['failed_id_match_rows'])} students with failed ID matches (matched by username only)")
    
    return results


# Keep the old function for backward compatibility
def merge_grades(lecture_files, lab_files, verbose=True):
    """
    Merge grades from lab gradebooks into lecture gradebooks.
    (Legacy function for backward compatibility)
    
    Args:
        lecture_files: List of paths to lecture CSV files
        lab_files: List of paths to lab CSV files
        verbose: Print diagnostic information
        
    Returns:
        Dictionary mapping lecture filenames to updated DataFrames
    """
    results = {
        'updated_dataframes': {},
        'stats': {},
        'students_not_found': [],
        'new_columns': [],
        'matched_students': set()
    }
    
    # Track all students seen in labs
    all_lab_students = set()
    
    # Load lecture gradebooks
    lecture_data = {}
    for filepath in lecture_files:
        df = load_csv_robust(filepath)
        lecture_data[Path(filepath).name] = df
        
        if verbose:
            print(f"\n📘 Loaded lecture file: {Path(filepath).name}")
            print(f"   Rows: {len(df)}, Columns: {len(df.columns)}")
    
    # Load lab gradebooks
    lab_data = {}
    for filepath in lab_files:
        df = load_csv_robust(filepath)
        lab_data[Path(filepath).name] = df
        
        if verbose:
            print(f"\n🧪 Loaded lab file: {Path(filepath).name}")
            print(f"   Rows: {len(df)}, Columns: {len(df.columns)}")
    
    # Process each lecture section
    for lecture_name, lecture_df in lecture_data.items():
        if verbose:
            print(f"\n{'='*60}")
            print(f"Processing lecture section: {lecture_name}")
            print('='*60)
        
        # Find student ID column
        lecture_id_col = find_student_id_column(lecture_df)
        if not lecture_id_col:
            print(f"❌ ERROR: Could not find student ID column in {lecture_name}")
            print(f"   Available columns: {', '.join(lecture_df.columns)}")
            continue
        
        if verbose:
            print(f"✓ Student ID column: '{lecture_id_col}'")
            sample_ids = lecture_df[lecture_id_col].dropna().head(5).tolist()
            print(f"   Sample IDs: {sample_ids}")
        
        # Create student ID lookup
        student_map = {}
        for idx, row in lecture_df.iterrows():
            student_id = row[lecture_id_col]
            student_id_str = normalize_student_id(student_id)
            if student_id_str:
                student_map[student_id_str] = idx
        
        if verbose:
            print(f"   Total students in lecture: {len(student_map)}")
        
        # Initialize stats for this section
        grades_updated = 0
        columns_added = 0
        
        # Process each lab gradebook
        for lab_name, lab_df in lab_data.items():
            if verbose:
                print(f"\n  Processing lab: {lab_name}")
            
            # Find student ID column in lab
            lab_id_col = find_student_id_column(lab_df)
            if not lab_id_col:
                print(f"  ❌ WARNING: Could not find student ID column in {lab_name}")
                print(f"     Available columns: {', '.join(lab_df.columns)}")
                continue
            
            if verbose:
                print(f"  ✓ Lab student ID column: '{lab_id_col}'")
                sample_lab_ids = lab_df[lab_id_col].dropna().head(5).tolist()
                print(f"     Sample lab IDs: {sample_lab_ids}")
            
            matched_this_lab = 0
            
            # Process each student in lab
            for _, lab_row in lab_df.iterrows():
                student_id = lab_row[lab_id_col]
                student_id_str = normalize_student_id(student_id)
                if not student_id_str:
                    continue
                
                # Track that we've seen this student in a lab
                all_lab_students.add(student_id_str)
                
                # Check if student exists in lecture
                if student_id_str not in student_map:
                    continue
                
                # Student found!
                matched_this_lab += 1
                results['matched_students'].add(student_id_str)
                lecture_row_idx = student_map[student_id_str]
                
                # Transfer grades for each column (except ID and name columns)
                for lab_col in lab_df.columns:
                    # Skip student ID and name columns
                    if lab_col == lab_id_col or 'name' in lab_col.lower():
                        continue
                    
                    # Skip non-assignment columns (Total, Last Accessed, etc.)
                    if not is_gradeable_assignment(lab_col):
                        continue
                    
                    grade = lab_row[lab_col]
                    if pd.isna(grade) or grade == '':
                        continue
                    
                    # Clean the assignment name (remove metadata like [Total Pts: ...])
                    cleaned_lab_col = clean_assignment_name(lab_col)
                    
                    # Find or create column in lecture data
                    lecture_col = find_matching_column(cleaned_lab_col, lecture_df.columns)
                    
                    if lecture_col is None:
                        # Create new column with cleaned name
                        lecture_col = cleaned_lab_col
                        lecture_df[lecture_col] = ''
                        columns_added += 1
                        if lecture_col not in results['new_columns']:
                            results['new_columns'].append(lecture_col)
                        if verbose:
                            print(f"     ➕ Created new column: '{lecture_col}' (from '{lab_col}')")
                    
                    # Update the grade
                    lecture_df.at[lecture_row_idx, lecture_col] = grade
                    grades_updated += 1
            
            if verbose:
                print(f"     Matched students in this lecture: {matched_this_lab}")
        
        # Store results
        results['updated_dataframes'][lecture_name] = lecture_df
        results['stats'][lecture_name] = {
            'grades_updated': grades_updated,
            'columns_added': columns_added
        }
        
        if verbose:
            print(f"\n  Summary for {lecture_name}:")
            print(f"    Grades updated: {grades_updated}")
            print(f"    Columns added: {columns_added}")
    
    # Now determine which students were truly not found in ANY lecture section
    results['students_not_found'] = sorted(list(all_lab_students - results['matched_students']))
    
    return results


def main():
    parser = argparse.ArgumentParser(
        description='Merge grades into lecture gradebooks',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage (Master gradebook mode):
    python merge_grades.py --lecture section_A.csv section_B.csv --master master_grades.csv
    
Example usage (Lab files mode):
    python merge_grades.py --lecture section_A.csv section_B.csv --lab lab_60.csv lab_61.csv
    python merge_grades.py -l sec_A.csv -b lab_60.csv lab_61.csv --output-dir merged/
        """
    )
    
    parser.add_argument(
        '--lecture', '-l',
        nargs='+',
        required=True,
        help='Lecture section CSV files'
    )
    
    # Mutually exclusive group for lab files or master file
    source_group = parser.add_mutually_exclusive_group(required=True)
    
    source_group.add_argument(
        '--master', '-m',
        help='Master gradebook CSV file (with Student ID and School email columns)'
    )
    
    source_group.add_argument(
        '--lab', '-b',
        nargs='+',
        help='Lab section CSV files (legacy mode)'
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
    
    # Verify source files exist
    if args.master:
        if not Path(args.master).exists():
            print(f"❌ ERROR: File not found: {args.master}")
            sys.exit(1)
    else:
        for filepath in args.lab:
            if not Path(filepath).exists():
                print(f"❌ ERROR: File not found: {filepath}")
                sys.exit(1)
    
    # Create output directory if needed
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Merge grades
    print("\n🎓 Grade Merger Script")
    print("=" * 60)
    
    if args.master:
        # Master gradebook mode
        results = merge_grades_from_master(
            lecture_files=args.lecture,
            master_file=args.master,
            verbose=not args.quiet
        )
        
        # Print summary with match methods
        print("\n" + "=" * 60)
        print("📊 MERGE SUMMARY")
        print("=" * 60)
        
        print(f"\n✓ Successfully matched {len(results['matched_students'])} unique students")
        print(f"   Matched by Student ID: {results['match_methods']['by_id']}")
        print(f"   Matched by Email: {results['match_methods']['by_email']}")
        
    else:
        # Lab files mode (legacy)
        results = merge_grades(
            lecture_files=args.lecture,
            lab_files=args.lab,
            verbose=not args.quiet
        )
        
        # Print summary
        print("\n" + "=" * 60)
        print("📊 MERGE SUMMARY")
        print("=" * 60)
        
        print(f"\n✓ Successfully matched {len(results['matched_students'])} unique students")
    
    if results['new_columns']:
        print(f"\n➕ New columns created: {', '.join(results['new_columns'])}")
    
    if results['students_not_found']:
        print(f"\n⚠️  Students not found in any lecture section ({len(results['students_not_found'])}):")
        for sid in results['students_not_found'][:10]:
            print(f"   - {sid}")
        if len(results['students_not_found']) > 10:
            print(f"   ... and {len(results['students_not_found']) - 10} more")
    
    # Write output files
    print("\n💾 Writing output files:")
    for lecture_name, df in results['updated_dataframes'].items():
        # Drop any Unnamed columns that may have snuck in
        unnamed_cols = [col for col in df.columns if 'Unnamed' in str(col)]
        if unnamed_cols:
            df = df.drop(columns=unnamed_cols)
        
        output_name = lecture_name.replace('.csv', '_merged.csv')
        output_path = output_dir / output_name
        df.to_csv(output_path, index=False)
        print(f"   ✓ {output_path}")
        stats = results['stats'][lecture_name]
        if 'matched_by_id' in stats:
            print(f"      Grades updated: {stats['grades_updated']}, Columns added: {stats['columns_added']}")
            print(f"      Matched: {stats['matched_by_id']} by ID, {stats['matched_by_email']} by email")
        else:
            print(f"      Grades updated: {stats['grades_updated']}, Columns added: {stats['columns_added']}")
    
    # Write orphaned students file if in master mode
    if args.master and 'orphaned_rows' in results and len(results['orphaned_rows']) > 0:
        orphaned_path = output_dir / 'orphaned_students.csv'
        results['orphaned_rows'].to_csv(orphaned_path, index=False)
        print(f"\n   ⚠️  {orphaned_path}")
        print(f"      {len(results['orphaned_rows'])} students not found in any lecture section")
    
    # Write failed ID match report if in master mode
    if args.master and 'failed_id_match_rows' in results and len(results['failed_id_match_rows']) > 0:
        failed_id_path = output_dir / 'failed_id_matches.csv'
        results['failed_id_match_rows'].to_csv(failed_id_path, index=False)
        print(f"\n   ⚠️  {failed_id_path}")
        print(f"      {len(results['failed_id_match_rows'])} students matched by username but ID match failed")
    
    print("\n✅ Done!")


if __name__ == '__main__':
    main()
