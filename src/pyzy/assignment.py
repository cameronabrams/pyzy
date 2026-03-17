"""
Assignment scorer - processes assignment pairs from before/after deadline lift
directories and generates consolidated audit and true zeros reports.
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


def _process_deadline_only(deadline_file, assignment_name, verbose=True):
    """
    Process a single deadline-only assignment (no lifted version available).

    Returns:
        Tuple of (df, audit_records, true_zero_records)
        audit_records is always empty; true_zero_records lists students with score 0.
    """
    if verbose:
        print(f"\nProcessing: {assignment_name}")
        print(f"   Deadline file: {deadline_file.name}")

    df = read_csv_with_trailing_comma_fix(deadline_file)

    id_col = find_student_id_column(df)
    email_col = find_email_column(df)

    score_col = None
    for col in df.columns:
        if 'percent score' in col.lower():
            score_col = col
            break

    due_date_col = None
    score_date_col = None
    for col in df.columns:
        if 'due date' in col.lower():
            due_date_col = col
        elif 'score date' in col.lower():
            score_date_col = col

    if verbose:
        print(f"   Total students: {len(df)}")

    true_zero_records = []

    if score_col and id_col:
        for _, row in df.iterrows():
            if row[score_col] == 0:
                first_name = ''
                last_name = ''
                for col in row.index:
                    if 'first' in col.lower() and 'name' in col.lower():
                        first_name = str(row[col]) if pd.notna(row[col]) else ''
                    elif 'last' in col.lower() and 'name' in col.lower():
                        last_name = str(row[col]) if pd.notna(row[col]) else ''
                true_zero_records.append({
                    'Assignment': assignment_name,
                    'Last Name': last_name,
                    'First Name': first_name,
                    'Student ID': normalize_student_id(row[id_col]),
                    'Due Date': row[due_date_col] if due_date_col else '',
                    'Score Date': row[score_date_col] if score_date_col else '',
                    'School Email': row[email_col] if email_col else '',
                })

    if verbose:
        print(f"   True zeros: {len(true_zero_records)}")

    return df, [], true_zero_records


def run_assignment(deadline_input, lifted_input=None, lecture_files=None,
                   output_dir='.', adjustments_file=None, quiet=False):
    """
    Run the assignment scoring workflow.

    deadline_input and lifted_input may each be a single CSV file or a directory.
    Mixing types (file + directory) is an error.

    Args:
        deadline_input: CSV file or directory with original-deadline CSVs
        lifted_input: CSV file or directory with lifted-deadline CSVs (optional)
        lecture_files: Gradebook CSV files to update with scored grades (optional)
        output_dir: Directory for output files (default: current directory)
        adjustments_file: YAML file with score adjustments (optional)
        quiet: Suppress verbose output
    """
    deadline_path = Path(deadline_input)

    if not deadline_path.exists():
        print(f"ERROR: Not found: {deadline_input}")
        sys.exit(1)

    deadline_is_file = deadline_path.is_file()
    deadline_only = lifted_input is None

    if not deadline_only:
        lifted_path = Path(lifted_input)
        if not lifted_path.exists():
            print(f"ERROR: Not found: {lifted_input}")
            sys.exit(1)
        lifted_is_file = lifted_path.is_file()

        if deadline_is_file != lifted_is_file:
            print("ERROR: --deadline and --lifted must both be files or both be directories.")
            sys.exit(1)

    print("\nAssignment Scorer")
    print("=" * 60)

    if deadline_only:
        if deadline_is_file:
            work_items = [(deadline_path, None, parse_assignment_filename_short(deadline_path.name))]
            print(f"\nSingle file (deadline only; no modifications): {deadline_path.name}")
        else:
            deadline_csvs = sorted(deadline_path.glob('*.csv'))
            if not deadline_csvs:
                print(f"\nERROR: No CSV files found in: {deadline_input}")
                sys.exit(1)
            work_items = [
                (f, None, parse_assignment_filename_short(f.name))
                for f in deadline_csvs
            ]
            print(f"\nFound {len(work_items)} file(s) in deadline directory (no modifications).")
    else:
        if deadline_is_file:
            assignment_name = parse_assignment_filename_short(deadline_path.name)
            work_items = [(deadline_path, lifted_path, assignment_name)]
            print(f"\nSingle assignment pair: {deadline_path.name} + {lifted_path.name}")
        else:
            work_items = match_files_between_directories(deadline_input, lifted_input)
            if not work_items:
                print(f"\nERROR: No matching files found between directories")
                print(f"   Deadline dir: {len(list(deadline_path.glob('*.csv')))} CSV files")
                print(f"   Lifted dir: {len(list(Path(lifted_input).glob('*.csv')))} CSV files")
                sys.exit(1)
            print(f"\nFound {len(work_items)} matching assignment pair(s)")

    adjustments = None
    if not deadline_only:
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
    elif adjustments_file:
        print("WARNING: --adjustments ignored when --lifted is not provided.")

    verbose = not quiet
    all_audit_records = []
    processed = []  # list of (assignment_name, merged_df)

    for deadline_file, lifted_file, assignment_name in work_items:
        try:
            if deadline_only:
                merged_df, audit_records, _ = _process_deadline_only(
                    deadline_file, assignment_name, verbose=verbose,
                )
            else:
                merged_df, audit_records, _ = merge_single_assignment(
                    deadline_file, lifted_file, assignment_name,
                    adjustments=adjustments, verbose=verbose,
                )
            all_audit_records.extend(audit_records)
            processed.append((assignment_name, merged_df))
        except Exception as e:
            print(f"\n   ERROR processing {assignment_name}: {e}")
            continue

    print("\n" + "=" * 60)
    print("ASSIGNMENT SCORING COMPLETE")
    print("=" * 60)

    print(f"\nProcessed {len(processed)} assignment(s):")
    for name, _ in processed:
        print(f"   - {name}")

    out = Path(output_dir)

    if all_audit_records:
        out.mkdir(parents=True, exist_ok=True)
        audit_df = pd.DataFrame(all_audit_records)
        audit_path = out / 'all_late_submissions.csv'
        audit_df.to_csv(audit_path, index=False, encoding='utf-8-sig')
        print(f"\nLate submissions: {len(audit_df)} record(s) -> {audit_path}")
        for name, _ in processed:
            count = len(audit_df[audit_df['Assignment'] == name])
            if count > 0:
                print(f"   {name}: {count}")
    else:
        print("\nNo late submissions.")

    if lecture_files and processed:
        from .activity import apply_scores_to_gradebook
        from .common import build_student_score_maps
        from .merge import find_username_column, sort_assignment_columns

        # Load gradebooks
        lecture_dfs = {}
        for lf in lecture_files:
            lf_path = Path(lf)
            if not lf_path.exists():
                print(f"ERROR: Gradebook not found: {lf}")
                continue
            lecture_dfs[lf_path.name] = read_csv_with_trailing_comma_fix(lf_path)

        if lecture_dfs:
            # Collect all usernames present in any gradebook for orphan detection
            all_lecture_usernames = set()
            for lf_df in lecture_dfs.values():
                un_col = find_username_column(lf_df)
                em_col = find_email_column(lf_df)
                for _, row in lf_df.iterrows():
                    if un_col and not pd.isna(row.get(un_col)):
                        all_lecture_usernames.add(str(row[un_col]).strip().lower())
                    elif em_col:
                        u = extract_username_from_email(row[em_col])
                        if u:
                            all_lecture_usernames.add(u)

            print("\n" + "=" * 60)
            print("GRADEBOOK UPDATE")
            print("=" * 60)

            for assignment_name, merged_df in processed:
                email_col = find_email_column(merged_df)
                score_col = next(
                    (c for c in merged_df.columns if 'percent score' in c.lower()), None
                )
                if not email_col or not score_col:
                    print(f"\n   WARNING: Cannot extract scores for '{assignment_name}' — skipping")
                    continue

                score_map, id_map, name_map = build_student_score_maps(merged_df, score_col)

                print(f"\n   Assignment: {assignment_name}")
                for lecture_name, lecture_df in lecture_dfs.items():
                    try:
                        _, updated = apply_scores_to_gradebook(
                            lecture_df, score_map, assignment_name, verbose=verbose,
                            id_score_map=id_map, name_score_map=name_map,
                        )
                        if verbose:
                            print(f"      {lecture_name}: {updated} updated")
                    except ValueError as e:
                        print(f"      ERROR ({lecture_name}): {e}")

                orphaned = [
                    {'Username': u, 'Score': score_map[u]}
                    for u in set(score_map.keys()) - all_lecture_usernames
                ]
                if orphaned:
                    out.mkdir(parents=True, exist_ok=True)
                    safe_name = assignment_name.replace(' ', '_')
                    orphaned_path = out / f'{safe_name}_orphaned.csv'
                    pd.DataFrame(orphaned).to_csv(orphaned_path, index=False, encoding='utf-8-sig')
                    print(f"      WARNING: {len(orphaned)} orphaned student(s) not found "
                          f"in any gradebook — see {orphaned_path}")

            out.mkdir(parents=True, exist_ok=True)
            print("\nWriting updated gradebooks:")
            for lecture_name, df in lecture_dfs.items():
                unnamed_cols = [col for col in df.columns if 'Unnamed' in str(col)]
                if unnamed_cols:
                    df = df.drop(columns=unnamed_cols)
                df = sort_assignment_columns(df)
                lec_id_col = find_student_id_column(df)
                if lec_id_col:
                    df[lec_id_col] = df[lec_id_col].apply(normalize_student_id)
                output_path = out / lecture_name.replace('.csv', '_merged.csv')
                df.to_csv(output_path, index=False, encoding='utf-8-sig')
                print(f"   {output_path}")

    print("\nDone!")
