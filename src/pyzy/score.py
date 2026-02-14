"""
Batch Assignment Deadline Lift Merger - processes assignment pairs from
before/after deadline lift directories and generates consolidated audit
and true zeros reports.
"""

import re
import sys
from pathlib import Path

import pandas as pd
import yaml

from .common import (
    find_email_column,
    find_student_id_column,
    extract_username_from_email,
    normalize_student_id,
    read_csv_with_trailing_comma_fix,
)


def parse_assignment_filename_short(filename):
    """Parse assignment name from zyBooks filename, returning abbreviated form only."""
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

        assignment_combo = f"{assignment_type} {assignment_kind}"
        abbrev_type = type_map.get(assignment_combo, None)

        if abbrev_type:
            return f"W{week_num} {abbrev_type}"

    return stem


def normalize_assignment_name(filename):
    """
    Normalize assignment name for matching between directories.
    Strips timestamps, report IDs, and common suffixes.
    """
    stem = Path(filename).stem.lower()

    if '_report' in stem:
        stem = stem.split('_report')[0]

    stem = re.sub(r'^.*?(?=week)', '', stem)

    for suffix in ['_before', '_after', '_deadline', '_lifted', '_original', '_extended']:
        stem = stem.replace(suffix, '')

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

    deadline_files = {normalize_assignment_name(f.name): f for f in deadline_path.glob('*.csv')}
    lifted_files = {normalize_assignment_name(f.name): f for f in lifted_path.glob('*.csv')}

    matches = []
    for norm_name, deadline_file in deadline_files.items():
        if norm_name in lifted_files:
            lifted_file = lifted_files[norm_name]
            assignment_name = parse_assignment_filename_short(deadline_file.name)
            matches.append((deadline_file, lifted_file, assignment_name))

    return matches


def late_score(row, adjustments: list[dict]) -> tuple[float, dict]:
    assignment = row['Assignment']
    week = assignment.split()[0]
    assn = assignment.split()[1]

    student_name = f'{row["First Name"]} {row["Last Name"]}'
    unpenalized_score = row['After Score']
    days_late = row['Days Late']
    no_adj = {'matched': False, 'rule': None, 'frac_deduction': 0.0}

    for adj in adjustments:
        penalty = adj.get('penalty', None)
        all_students = 'student' not in adj
        all_assn = 'assignment' not in adj
        adj_week = f'W{adj["week"]}'

        if week != adj_week:
            continue

        if all_students and all_assn:
            if not penalty:
                info = {
                    'matched': True,
                    'rule': f'{week} all students, all assignments: penalty waived',
                    'frac_deduction': 0.0,
                }
                return unpenalized_score, info
        elif all_students and not all_assn:
            if assn == adj['assignment']:
                if not penalty:
                    info = {
                        'matched': True,
                        'rule': f'{week} all students, {assn}: penalty waived',
                        'frac_deduction': 0.0,
                    }
                    return unpenalized_score, info
                else:
                    for p in penalty:
                        if 'days-late' in p:
                            if days_late <= p['days-late']:
                                frac = p['frac-deduction']
                                info = {
                                    'matched': True,
                                    'rule': (
                                        f'{week} all students, {assn}: {frac} deduction '
                                        f'(days late <= {p["days-late"]})'
                                    ),
                                    'frac_deduction': frac,
                                }
                                return (1 - frac) * unpenalized_score, info
        elif not all_students and all_assn:
            if student_name == adj['student']:
                if not penalty:
                    info = {
                        'matched': True,
                        'rule': f'{week} {student_name}, all assignments: penalty waived',
                        'frac_deduction': 0.0,
                    }
                    return unpenalized_score, info
                else:
                    for p in penalty:
                        if 'days-late' in p:
                            if days_late <= p['days-late']:
                                frac = p['frac-deduction']
                                info = {
                                    'matched': True,
                                    'rule': (
                                        f'{week} {student_name}, all assignments: {frac} '
                                        f'deduction (days late <= {p["days-late"]})'
                                    ),
                                    'frac_deduction': frac,
                                }
                                return (1 - frac) * unpenalized_score, info
        elif not all_students and not all_assn:
            if student_name == adj['student'] and assn == adj['assignment']:
                if not penalty:
                    info = {
                        'matched': True,
                        'rule': f'{week} {student_name}, {assn}: penalty waived',
                        'frac_deduction': 0.0,
                    }
                    return unpenalized_score, info
                else:
                    for p in penalty:
                        if 'days-late' in p:
                            if days_late <= p['days-late']:
                                frac = p['frac-deduction']
                                info = {
                                    'matched': True,
                                    'rule': (
                                        f'{week} {student_name}, {assn}: {frac} deduction '
                                        f'(days late <= {p["days-late"]})'
                                    ),
                                    'frac_deduction': frac,
                                }
                                return (1 - frac) * unpenalized_score, info

    return unpenalized_score, no_adj


def merge_single_assignment(before_file, after_file, assignment_name,
                            adjustments=None, verbose=True):
    """
    Merge a single assignment pair.

    Returns:
        Tuple of (merged_df, audit_records, true_zero_records)
    """
    if verbose:
        print(f"\nProcessing: {assignment_name}")
        print(f"   Deadline file: {before_file.name}")
        print(f"   Lifted file:   {after_file.name}")

    before_df = read_csv_with_trailing_comma_fix(before_file)
    after_df = read_csv_with_trailing_comma_fix(after_file)

    before_id_col = find_student_id_column(before_df)
    before_email_col = find_email_column(before_df)
    after_id_col = find_student_id_column(after_df)
    after_email_col = find_email_column(after_df)

    if not before_id_col or not after_id_col:
        raise ValueError(
            f"Could not find Student ID column in {before_file.name} or {after_file.name}"
        )

    score_col = None
    for col in before_df.columns:
        if 'percent score' in col.lower():
            score_col = col
            break

    if not score_col:
        raise ValueError(f"Could not find Percent score column in {before_file.name}")

    due_date_col = None
    score_date_col = None
    for col in before_df.columns:
        if 'due date' in col.lower():
            due_date_col = col
        elif 'score date' in col.lower():
            score_date_col = col

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

    merged_df = before_df.copy()

    audit_records = []
    true_zero_records = []

    if verbose:
        print(f"   Total students: {len(before_df)}")

    updates_made = 0
    true_zeros = 0

    for before_idx, before_row in before_df.iterrows():
        student_id = normalize_student_id(before_row[before_id_col])
        username = None
        if before_email_col:
            username = extract_username_from_email(before_row[before_email_col])

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

        after_row = after_df.loc[after_idx]
        after_score = after_row[score_col]
        before_score = before_row[score_col]
        after_score_date = after_row[score_date_col] if score_date_col else None

        first_name = ''
        last_name = ''
        for col in before_row.index:
            if 'first' in col.lower() and 'name' in col.lower():
                first_name = str(before_row[col]) if pd.notna(before_row[col]) else ''
            elif 'last' in col.lower() and 'name' in col.lower():
                last_name = str(before_row[col]) if pd.notna(before_row[col]) else ''

        if before_score == 0 and after_score == 0:
            true_zeros += 1
            true_zero_records.append({
                'Assignment': assignment_name,
                'Last Name': last_name,
                'First Name': first_name,
                'Student ID': student_id,
                'Due Date': before_row[due_date_col] if due_date_col else '',
                'Score Date': before_row[score_date_col] if score_date_col else '',
                'School Email': before_row[before_email_col] if before_email_col else '',
            })
            continue

        applied_score = after_score
        days_late = 0
        adjustment_info = {'matched': False, 'rule': None, 'frac_deduction': 0.0}

        if adjustments is not None and due_date_col and score_date_col and after_score_date:
            try:
                due_date_str = str(before_row[due_date_col]).replace(' EST', '')
                after_score_date_str = str(after_score_date).replace(' EST', '')
                due_date = pd.to_datetime(due_date_str)
                score_date_dt = pd.to_datetime(after_score_date_str)
                days_late = round(
                    (score_date_dt - due_date).total_seconds() / 3600 / 24, 1
                )
            except Exception:
                days_late = 0

            late_row = {
                'Assignment': assignment_name,
                'First Name': first_name,
                'Last Name': last_name,
                'After Score': after_score,
                'Days Late': days_late,
            }
            applied_score, adjustment_info = late_score(late_row, adjustments)

        merged_df.at[before_idx, score_col] = applied_score
        if score_date_col and after_score_date:
            merged_df.at[before_idx, score_date_col] = after_score_date

        updates_made += 1

        audit_records.append({
            'Assignment': assignment_name,
            'Last Name': last_name,
            'First Name': first_name,
            'Student ID': student_id,
            'Match Method': match_method,
            'Before Score': before_score,
            'After Score': after_score,
            'Applied Score': applied_score,
            'Days Late': days_late,
            'Adjustment Matched': adjustment_info['matched'],
            'Adjustment Rule': adjustment_info['rule'] or '',
            'Frac Deduction': adjustment_info['frac_deduction'],
            'Before Score Date': before_row[score_date_col] if score_date_col else '',
            'After Score Date': after_score_date if after_score_date else '',
            'Due Date': before_row[due_date_col] if due_date_col else '',
        })

    if verbose:
        print(f"   Updates: {updates_made}, True zeros: {true_zeros}")

    return merged_df, audit_records, true_zero_records


def run_score(deadline_dir, lifted_dir, output_dir='merged',
              adjustments_file=None, quiet=False):
    """
    Run the score (deadline-lift merge) workflow.

    Args:
        deadline_dir: Directory with original-deadline CSVs
        lifted_dir: Directory with lifted-deadline CSVs
        output_dir: Output directory for merged files
        adjustments_file: YAML file with score adjustments (optional)
        quiet: Suppress verbose output
    """
    deadline_path = Path(deadline_dir)
    lifted_path = Path(lifted_dir)

    if not deadline_path.exists():
        print(f"ERROR: Deadline directory not found: {deadline_dir}")
        sys.exit(1)

    if not lifted_path.exists():
        print(f"ERROR: Lifted directory not found: {lifted_dir}")
        sys.exit(1)

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    print("\nBatch Assignment Deadline Lift Merger")
    print("=" * 60)

    matches = match_files_between_directories(deadline_dir, lifted_dir)

    if len(matches) == 0:
        print(f"\nERROR: No matching files found between directories")
        print(f"   Deadline dir: {len(list(deadline_path.glob('*.csv')))} CSV files")
        print(f"   Lifted dir: {len(list(lifted_path.glob('*.csv')))} CSV files")
        sys.exit(1)

    print(f"\nFound {len(matches)} matching assignment pairs")

    adjustments = None
    if adjustments_file:
        adj_path = Path(adjustments_file)
        if not adj_path.exists():
            print(f"ERROR: Adjustments file not found: {adjustments_file}")
            sys.exit(1)
        with open(adj_path, 'r', encoding='utf-8') as f:
            adjustments = yaml.safe_load(f)
        print(f"Loaded adjustments from: {adjustments_file}")
    else:
        print("No adjustments file specified; late penalties will not be applied.")

    all_audit_records = []
    all_true_zero_records = []
    processed_assignments = []

    for deadline_file, lifted_file, assignment_name in matches:
        try:
            merged_df, audit_records, true_zero_records = merge_single_assignment(
                deadline_file,
                lifted_file,
                assignment_name,
                adjustments=adjustments,
                verbose=not quiet,
            )

            merged_filename = f"{assignment_name}_merged.csv"
            merged_path = out / merged_filename
            merged_df.to_csv(merged_path, index=False, encoding='utf-8-sig')

            all_audit_records.extend(audit_records)
            all_true_zero_records.extend(true_zero_records)
            processed_assignments.append(assignment_name)

        except Exception as e:
            print(f"\n   ERROR processing {assignment_name}: {e}")
            continue

    print("\n" + "=" * 60)
    print("BATCH MERGE COMPLETE")
    print("=" * 60)

    print(f"\nProcessed {len(processed_assignments)} assignments:")
    for assignment in processed_assignments:
        print(f"   - {assignment}")

    audit_df = pd.DataFrame()
    if len(all_audit_records) > 0:
        audit_df = pd.DataFrame(all_audit_records)
        audit_path = out / 'all_late_submissions.csv'
        audit_df.to_csv(audit_path, index=False, encoding='utf-8-sig')

        print(f"\nConsolidated audit report: {audit_path}")
        print(f"   Total late submissions: {len(audit_df)}")

        print(f"\n   Late submissions by assignment:")
        for assignment in processed_assignments:
            count = len(audit_df[audit_df['Assignment'] == assignment])
            if count > 0:
                print(f"      {assignment}: {count}")
    else:
        print(f"\n   No late submissions found")

    for assignment in processed_assignments:
        merged_path = out / f'{assignment}_merged.csv'
        assignment_data = pd.read_csv(merged_path, header=0, encoding='utf-8-sig')
        assignment_data['Username'] = assignment_data['School email'].str.replace(
            '@drexel.edu', '', regex=False
        )

        if not audit_df.empty:
            this_lates = audit_df[audit_df['Assignment'] == assignment]
            for row, late in this_lates.iterrows():
                student_id = int(late['Student ID'])
                score = late['Applied Score']
                assignment_data.loc[
                    assignment_data['Student ID'] == student_id, 'Percent score'
                ] = score
            scored_path = out / f'{assignment}_scored.csv'
            assignment_data.to_csv(scored_path, index=False)

        bblearn = assignment_data[[
            'Last name', 'First name', 'Primary email',
            'School email', 'Student ID', 'Percent score',
        ]]
        bblearn_path = out / f'{assignment}_bblearn.csv'
        bblearn.to_csv(bblearn_path, index=False)
        print(f'BBLearn upload file saved: {bblearn_path}')

    if len(all_true_zero_records) > 0:
        true_zeros_df = pd.DataFrame(all_true_zero_records)
        true_zeros_path = out / 'all_true_zeros.csv'
        true_zeros_df.to_csv(true_zeros_path, index=False, encoding='utf-8-sig')

        print(f"\nConsolidated true zeros report: {true_zeros_path}")
        print(f"   Total non-submissions: {len(true_zeros_df)}")

        print(f"\n   Non-submissions by assignment:")
        for assignment in processed_assignments:
            count = len(true_zeros_df[true_zeros_df['Assignment'] == assignment])
            if count > 0:
                print(f"      {assignment}: {count}")

        if len(true_zeros_df) > 0:
            non_submission_counts = (
                true_zeros_df.groupby('Student ID').size().sort_values(ascending=False)
            )
            if len(non_submission_counts) > 0:
                print(f"\n   Students with multiple non-submissions:")
                for student_id, count in non_submission_counts.head(10).items():
                    if count > 1:
                        student_rows = true_zeros_df[
                            true_zeros_df['Student ID'] == student_id
                        ]
                        if len(student_rows) > 0:
                            name = (
                                f"{student_rows.iloc[0]['First Name']} "
                                f"{student_rows.iloc[0]['Last Name']}"
                            )
                            print(f"      {name} ({student_id}): {count} assignments")
    else:
        print(f"\n   No true zeros (all students submitted or submitted late)")

    print(f"\nIndividual merged files saved to: {out}/")
    print("\nDone!")
