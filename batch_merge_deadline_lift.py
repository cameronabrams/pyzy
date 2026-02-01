#!/usr/bin/env python3
"""
Batch Assignment Deadline Lift Merger
Processes all assignment pairs from two directories (before/after deadline lift)
and generates consolidated audit and true zeros reports.

Usage:
    python batch_merge_deadline_lift.py --deadline deadline/ --lifted lifted/
    python batch_merge_deadline_lift.py --deadline before/ --lifted after/ --output merged/

Output:
    - Individual merged CSV files for each assignment
    - Single consolidated audit report (all late submissions)
    - Single consolidated true zeros report (all non-submissions)
"""

import argparse
import sys
import re
from pathlib import Path
import pandas as pd
import yaml
import numpy as np

def normalize_student_id(student_id):
    """Normalize student ID to a string, handling float/int conversions."""
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
    """Extract username from email address."""
    if pd.isna(email) or not email or '@' not in str(email):
        return ''
    
    return str(email).split('@')[0].strip().lower()


def find_email_column(df):
    """Find the SCHOOL email column in a DataFrame."""
    patterns = ['school email', 'schoolemail', 'school_email']
    
    for col in df.columns:
        col_lower = col.lower().replace(' ', '').replace('_', '').replace('-', '')
        for pattern in patterns:
            pattern_clean = pattern.replace(' ', '').replace('_', '').replace('-', '')
            if pattern_clean == col_lower:
                return col
    
    return None


def read_csv_with_trailing_comma_fix(filepath):
    """Read a CSV file that may have trailing commas on each line."""
    import io
    
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        lines = f.readlines()
    
    fixed_lines = []
    for line in lines:
        line = line.rstrip('\n\r')
        if line.endswith(','):
            line = line[:-1]
        fixed_lines.append(line + '\n')
    
    return pd.read_csv(io.StringIO(''.join(fixed_lines)))


def parse_assignment_filename(filename):
    """Parse assignment name from zyBooks filename."""
    stem = Path(filename).stem
    
    # Try zyBooks format
    match = re.search(r'Week[_\s]+(\d+)[_\s]+(Participation|Challenge|In-Lab|Out-of-Lab)[_\s]+(Activities|Labs)', 
                     stem, re.IGNORECASE)
    
    if match:
        week_num = match.group(1)
        assignment_type = match.group(2).strip()
        assignment_kind = match.group(3).strip()
        
        type_map = {
            'Participation Activities': 'PA',
            'Challenge Activities': 'CA',
            'In-Lab Labs': 'IL',
            'Out-of-Lab Labs': 'OL'
        }
        
        assignment_combo = f"{assignment_type} {assignment_kind}"
        abbrev_type = type_map.get(assignment_combo, None)
        
        if abbrev_type:
            return f"W{week_num} {abbrev_type}"
    
    return stem


def normalize_assignment_name(filename):
    """
    Normalize assignment name for matching between directories.
    Strips timestamps, report IDs, and common suffixes.
    
    Examples:
        DREXELENGR131Winter2026_Week_1_Challenge_Activities_report_103126_2026-02-01_140652.csv
        → week_1_challenge_activities
        
        Week_1_Challenge_Activities_before.csv
        → week_1_challenge_activities
    """
    stem = Path(filename).stem.lower()
    
    # Strip everything after "report" (zyBooks timestamp pattern)
    if '_report' in stem:
        stem = stem.split('_report')[0]
    
    # Strip course prefix patterns (e.g., "drexelengr131winter2026_")
    # More flexible: remove any alphanumeric prefix before "week"
    stem = re.sub(r'^.*?(?=week)', '', stem)
    
    # Remove common suffixes
    for suffix in ['_before', '_after', '_deadline', '_lifted', '_original', '_extended']:
        stem = stem.replace(suffix, '')
    
    # Clean up any double underscores
    stem = re.sub(r'_+', '_', stem)
    
    return stem.strip('_')


def match_files_between_directories(deadline_dir, lifted_dir):
    """
    Match files between deadline and lifted directories.
    
    Returns:
        List of tuples (deadline_file, lifted_file, assignment_name)
    """
    deadline_path = Path(deadline_dir)
    lifted_path = Path(lifted_dir)
    
    # Get all CSV files
    deadline_files = {normalize_assignment_name(f.name): f for f in deadline_path.glob('*.csv')}
    lifted_files = {normalize_assignment_name(f.name): f for f in lifted_path.glob('*.csv')}
    
    # Find matches
    matches = []
    for norm_name, deadline_file in deadline_files.items():
        if norm_name in lifted_files:
            lifted_file = lifted_files[norm_name]
            assignment_name = parse_assignment_filename(deadline_file.name)
            matches.append((deadline_file, lifted_file, assignment_name))
    
    return matches


def late_score(row, adjustments: list[dict] ) -> float:
    assignment = row['Assignment']
    week = assignment.split()[0]  # e.g., 'W1' or 'W2'
    assn = assignment.split()[1]  # e.g., 'PA'"W1 CA_merged.csv"

    student_name = f'{row['First Name']} {row['Last Name']}'
    unpenalized_score = row['After Score']
    days_late = row['Days Late']
    hours_late = days_late * 24
    for adj in adjustments:
        penalty = adj.get('penalty', None)
        all_students = 'student' not in adj
        all_assn = 'assignment' not in adj
        adj_week = f'W{adj["week"]}'
        if week == adj_week:
            if all_students and all_assn:
                if not penalty:
                    # print(f"No penalty applied for all students on all assignments in week {week}")
                    return unpenalized_score
            elif all_students and not all_assn:
                if assn == adj['assignment']:
                    if not penalty:
                        # print(f"No penalty applied for all students on {assignment} in week {week}")
                        return unpenalized_score
                    else:
                        for penalty in penalty:
                            if 'days-late' in penalty:
                                if days_late <= penalty['days-late']:
                                    # print(f"Applying penalty for all students on {assignment} in week {week}: {penalty['frac-deduction']}")
                                    return (1 - penalty['frac-deduction']) * unpenalized_score
            elif not all_students and all_assn:
                if student_name == adj['student']:
                    if not penalty:
                        # print(f"No penalty applied for {student_name} on all assignments in week {week}")
                        return unpenalized_score
                    else:
                        for penalty in penalty:
                            if 'days-late' in penalty:
                                if days_late <= penalty['days-late']:
                                    # print(f"Applying penalty for {student_name} on all assignments in week {week}: {penalty['frac-deduction']}")
                                    return (1 - penalty['frac-deduction']) * unpenalized_score
            elif not all_students and not all_assn:
                if student_name == adj['student'] and assn == adj['assignment']:
                    if not penalty:
                        # print(f"No penalty applied for {student_name} on {assignment} in week {week}")
                        return unpenalized_score
                    else:
                        for penalty in penalty:
                            if 'days-late' in penalty:
                                if days_late <= penalty['days-late']:
                                    # print(f"Applying penalty for {student_name} on {assignment} in week {week}: {penalty['frac-deduction']}")
                                    return (1 - penalty['frac-deduction']) * unpenalized_score
        else:
            continue
    return unpenalized_score

def merge_single_assignment(before_file, after_file, assignment_name, verbose=True):
    """
    Merge a single assignment pair.
    
    Returns:
        Tuple of (merged_df, audit_records, true_zero_records)
    """
    if verbose:
        print(f"\n📝 Processing: {assignment_name}")
        print(f"   Deadline file: {before_file.name}")
        print(f"   Lifted file:   {after_file.name}")
    
    # Load both files
    before_df = read_csv_with_trailing_comma_fix(before_file)
    after_df = read_csv_with_trailing_comma_fix(after_file)
    
    # Find required columns
    before_id_col = find_student_id_column(before_df)
    before_email_col = find_email_column(before_df)
    after_id_col = find_student_id_column(after_df)
    after_email_col = find_email_column(after_df)
    
    if not before_id_col or not after_id_col:
        raise ValueError(f"Could not find Student ID column in {before_file.name} or {after_file.name}")
    
    # Find score columns
    score_col = None
    for col in before_df.columns:
        if 'percent score' in col.lower():
            score_col = col
            break
    
    if not score_col:
        raise ValueError(f"Could not find Percent score column in {before_file.name}")
    
    # Find date columns
    due_date_col = None
    score_date_col = None
    for col in before_df.columns:
        if 'due date' in col.lower():
            due_date_col = col
        elif 'score date' in col.lower():
            score_date_col = col
    
    # Build lookup map for after file
    after_map_id = {}
    after_map_username = {}
    
    for idx, row in after_df.iterrows():
        student_id = normalize_student_id(row[after_id_col])
        if student_id:
            after_map_id[student_id] = idx
        
        if after_email_col:
            username = extract_username_from_email(row[after_email_col])
            if username:
                after_map_username[username] = idx
    
    # Create merged dataframe
    merged_df = before_df.copy()
    
    # Track changes
    audit_records = []
    true_zero_records = []
    
    # Find students with zeros
    zero_scores = before_df[before_df[score_col] == 0]
    
    if verbose:
        print(f"   Students with zero scores: {len(zero_scores)}")
    
    updates_made = 0
    true_zeros = 0
    
    # Process each zero-score student
    for before_idx, before_row in zero_scores.iterrows():
        student_id = normalize_student_id(before_row[before_id_col])
        username = None
        if before_email_col:
            username = extract_username_from_email(before_row[before_email_col])
        
        # Try to find in after file
        after_idx = None
        match_method = None
        
        if student_id and student_id in after_map_id:
            after_idx = after_map_id[student_id]
            match_method = 'ID'
        elif username and username in after_map_username:
            after_idx = after_map_username[username]
            match_method = 'username'
        
        if after_idx is None:
            continue
        
        # Get after data
        after_row = after_df.loc[after_idx]
        after_score = after_row[score_col]
        after_score_date = after_row[score_date_col] if score_date_col else None
        
        # Get student name
        first_name = ''
        last_name = ''
        for col in before_row.index:
            if 'first' in col.lower() and 'name' in col.lower():
                first_name = str(before_row[col]) if pd.notna(before_row[col]) else ''
            elif 'last' in col.lower() and 'name' in col.lower():
                last_name = str(before_row[col]) if pd.notna(before_row[col]) else ''
        
        # Check if still zero (true zero)
        if after_score == 0:
            true_zeros += 1
            true_zero_records.append({
                'Assignment': assignment_name,
                'Last Name': last_name,
                'First Name': first_name,
                'Student ID': student_id,
                'Due Date': before_row[due_date_col] if due_date_col else '',
                'Score Date': before_row[score_date_col] if score_date_col else '',
                'School Email': before_row[before_email_col] if before_email_col else ''
            })
            continue
        
        # Update merged dataframe
        merged_df.at[before_idx, score_col] = after_score
        if score_date_col and after_score_date:
            merged_df.at[before_idx, score_date_col] = after_score_date
        
        updates_made += 1
        
        # Record audit data
        audit_records.append({
            'Assignment': assignment_name,
            'Last Name': last_name,
            'First Name': first_name,
            'Student ID': student_id,
            'Match Method': match_method,
            'Before Score': 0,
            'After Score': after_score,
            'Before Score Date': before_row[score_date_col] if score_date_col else '',
            'After Score Date': after_score_date if after_score_date else '',
            'Due Date': before_row[due_date_col] if due_date_col else ''
        })
    
    if verbose:
        print(f"   Updates: {updates_made}, True zeros: {true_zeros}")
    
    return merged_df, audit_records, true_zero_records


def main():
    parser = argparse.ArgumentParser(
        description='Batch process assignment deadline lift merges',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
    # Basic usage
    python batch_merge_deadline_lift.py --deadline deadline/ --lifted lifted/
    
    # Custom output directory
    python batch_merge_deadline_lift.py --deadline deadline/ --lifted lifted/ --output-dir merged/
    
Expected directory structure:
    deadline/
        Week_1_Participation_Activities.csv
        Week_1_Challenge_Activities.csv
        Week_2_Participation_Activities.csv
        ...
    lifted/
        Week_1_Participation_Activities.csv
        Week_1_Challenge_Activities.csv
        Week_2_Participation_Activities.csv
        ...
        """
    )
    
    parser.add_argument(
        '--deadline', '-d',
        required=True,
        help='Directory containing assignment CSVs with original due dates'
    )
    
    parser.add_argument(
        '--lifted', '-l',
        required=True,
        help='Directory containing assignment CSVs with lifted deadlines'
    )
    
    parser.add_argument(
        '--output-dir', '-o',
        default='merged',
        help='Output directory for merged files (default: merged/)'
    )
    
    parser.add_argument(
        '--adjustments', '-a',
        default = 'adjustments.yaml',
        help='YAML file containing adjustments for scores (default: adjustments.yaml)'
    )

    parser.add_argument(
        '--quiet', '-q',
        action='store_true',
        help='Suppress verbose output'
    )
    
    args = parser.parse_args()
    
    # Verify directories exist
    deadline_path = Path(args.deadline)
    lifted_path = Path(args.lifted)
    
    if not deadline_path.exists():
        print(f"❌ ERROR: Deadline directory not found: {args.deadline}")
        sys.exit(1)
    
    if not lifted_path.exists():
        print(f"❌ ERROR: Lifted directory not found: {args.lifted}")
        sys.exit(1)
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("\n📋 Batch Assignment Deadline Lift Merger")
    print("=" * 60)
    
    # Match files between directories
    matches = match_files_between_directories(args.deadline, args.lifted)
    
    if len(matches) == 0:
        print(f"\n❌ ERROR: No matching files found between directories")
        print(f"   Deadline dir: {len(list(deadline_path.glob('*.csv')))} CSV files")
        print(f"   Lifted dir: {len(list(lifted_path.glob('*.csv')))} CSV files")
        sys.exit(1)
    
    print(f"\nFound {len(matches)} matching assignment pairs")
    
    # Process all assignments
    all_audit_records = []
    all_true_zero_records = []
    processed_assignments = []
    
    for deadline_file, lifted_file, assignment_name in matches:
        try:
            merged_df, audit_records, true_zero_records = merge_single_assignment(
                deadline_file,
                lifted_file,
                assignment_name,
                verbose=not args.quiet
            )
            
            # Save merged file
            merged_filename = f"{assignment_name}_merged.csv"
            merged_path = output_dir / merged_filename
            merged_df.to_csv(merged_path, index=False, encoding='utf-8-sig')
            
            # Accumulate records
            all_audit_records.extend(audit_records)
            all_true_zero_records.extend(true_zero_records)
            processed_assignments.append(assignment_name)
            
        except Exception as e:
            print(f"\n   ❌ ERROR processing {assignment_name}: {e}")
            continue
    
    # Create consolidated reports
    print("\n" + "=" * 60)
    print("📊 BATCH MERGE COMPLETE")
    print("=" * 60)
    
    print(f"\n✓ Processed {len(processed_assignments)} assignments:")
    for assignment in processed_assignments:
        print(f"   - {assignment}")
    
    # Save consolidated audit report
    audit_df = pd.DataFrame()
    if len(all_audit_records) > 0:
        audit_df = pd.DataFrame(all_audit_records)
        audit_df['After Score Date'] = audit_df['After Score Date'].str.replace(' EST','', regex=False)
        audit_df['Due Date'] = audit_df['Due Date'].str.replace(' EST','', regex=False)
        # ensure "after score date" and "due date" are datetime objects
        audit_df['After Score Date'] = pd.to_datetime(audit_df['After Score Date'])
        audit_df['Due Date'] = pd.to_datetime(audit_df['Due Date'])
        audit_df['Days Late'] = np.round((audit_df['After Score Date'] - audit_df['Due Date']).dt.total_seconds() / 3600 / 24, 1)
        with open(args.adjustments, 'r', encoding='utf-8') as f:
            adjustments = yaml.safe_load(f)
        audit_df['Applied Score'] = audit_df.apply(lambda row: late_score(row, adjustments), axis=1)
        audit_path = output_dir / 'all_late_submissions.csv'
        audit_df.to_csv(audit_path, index=False, encoding='utf-8-sig')
        
        print(f"\n💾 Consolidated audit report: {audit_path}")
        print(f"   Total late submissions: {len(audit_df)}")
        
        # Summary by assignment
        print(f"\n   Late submissions by assignment:")
        for assignment in processed_assignments:
            count = len(audit_df[audit_df['Assignment'] == assignment])
            if count > 0:
                print(f"      {assignment}: {count}")
    else:
        print(f"\n   No late submissions found")
    
    # generate bblearn-format per-assignment files for later merging
    for assignment in processed_assignments:
        merged_path = output_dir / f'{assignment}_merged.csv'
        assignment_data = pd.read_csv(merged_path, header=0, encoding='utf-8-sig')
        # strip "@drexel.edu" from School email and save as "Username"
        assignment_data['Username'] = assignment_data['School email'].str.replace('@drexel.edu','', regex=False)
        # print(f'{assignment_data.head().to_string()}')
        if not audit_df.empty:
            this_lates = audit_df[audit_df['Assignment'] == assignment]
            # print(f'\nProcessing {len(this_lates)} late submissions for {assignment} from {merged_path.name}')
            for row, late in this_lates.iterrows():
                student_id = int(late['Student ID'])
                score = late['Applied Score']
                name = f"{late['First Name']} {late['Last Name']}"
                # print(f'  Processing late submission for Student ID {student_id} ({name}): score {score}')
                before = assignment_data.loc[assignment_data['Student ID'] == student_id, 'Percent score'].values[0]
                assignment_data.loc[assignment_data['Student ID'] == student_id, 'Percent score'] = score
            # print(f'    updated Student ID {student_id} ({name}) from score {before} to score {score}')
            scored_path = output_dir / f'{assignment}_scored.csv'
            assignment_data.to_csv(scored_path, index=False)
        # Last name,First name,Primary email,School email,Student ID
        bblearn = assignment_data[['Last name', 'First name', 'Primary email', 'School email', 'Student ID', 'Percent score']]
        # rename columns for BBLearn upload
        # "Last Name","First Name","Username","Student ID"
        bblean_path = output_dir / f'{assignment}_bblearn.csv'
        bblearn.to_csv(bblean_path, index=False)
        print(f'💾 BBLearn upload file saved: {bblean_path}')

    # Save consolidated true zeros report
    if len(all_true_zero_records) > 0:
        true_zeros_df = pd.DataFrame(all_true_zero_records)
        true_zeros_path = output_dir / 'all_true_zeros.csv'
        true_zeros_df.to_csv(true_zeros_path, index=False, encoding='utf-8-sig')
        
        print(f"\n💾 Consolidated true zeros report: {true_zeros_path}")
        print(f"   Total non-submissions: {len(true_zeros_df)}")
        
        # Summary by assignment
        print(f"\n   Non-submissions by assignment:")
        for assignment in processed_assignments:
            count = len(true_zeros_df[true_zeros_df['Assignment'] == assignment])
            if count > 0:
                print(f"      {assignment}: {count}")
        
        # Top non-submitters
        if len(true_zeros_df) > 0:
            non_submission_counts = true_zeros_df.groupby('Student ID').size().sort_values(ascending=False)
            if len(non_submission_counts) > 0:
                print(f"\n   Students with multiple non-submissions:")
                for student_id, count in non_submission_counts.head(10).items():
                    if count > 1:
                        # Get student name
                        student_rows = true_zeros_df[true_zeros_df['Student ID'] == student_id]
                        if len(student_rows) > 0:
                            name = f"{student_rows.iloc[0]['First Name']} {student_rows.iloc[0]['Last Name']}"
                            print(f"      {name} ({student_id}): {count} assignments")
    else:
        print(f"\n   No true zeros (all students submitted or submitted late)")
    
    print(f"\n💾 Individual merged files saved to: {output_dir}/")
    print("\n✅ Done!")


if __name__ == '__main__':
    main()