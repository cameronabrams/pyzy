"""
Late penalty adjuster - reads *_late.csv reports produced by the assignment
subcommand and lets the instructor interactively override per-student scores.
"""

import csv
import sys
from pathlib import Path

import pandas as pd

from .common import (
    extract_username_from_email,
    find_email_column,
    find_student_id_column,
    load_weights_csv,
    normalize_student_id,
    read_csv_with_trailing_comma_fix,
    recompute_averages,
    resolve_column,
)
from .merge import find_username_column, sort_assignment_columns


def _assignment_name_from_late_file(filename):
    """
    Extract assignment name from a late report filename.

    'W3_PA_late.csv' -> 'W3 PA'
    """
    stem = Path(filename).stem
    if stem.endswith('_late'):
        return stem[:-5].replace('_', ' ')
    return stem.replace('_', ' ')


def _find_student_row(lecture_df, student_id, email):
    """Return the DataFrame index for a student matched by ID then username."""
    id_col = find_student_id_column(lecture_df)
    email_col = find_email_column(lecture_df)
    un_col = find_username_column(lecture_df)
    target_username = extract_username_from_email(email) if email else None

    for idx, row in lecture_df.iterrows():
        if id_col and student_id:
            if normalize_student_id(row[id_col]) == student_id:
                return idx
        if target_username:
            username = None
            if un_col and not pd.isna(row.get(un_col)):
                username = str(row[un_col]).strip().lower()
            elif email_col:
                username = extract_username_from_email(row[email_col])
            if username == target_username:
                return idx
    return None


def _matches_student(row, query):
    """Return True if the row matches the query (ID, username, or 'Last, First')."""
    q = query.strip().lower()
    sid = normalize_student_id(row.get('Student ID', ''))
    if sid and sid == normalize_student_id(query):
        return True
    email = str(row.get('School Email', '')).strip().lower()
    username = extract_username_from_email(email) if email else ''
    if username and username == q:
        return True
    last = str(row.get('Last Name', '')).strip().lower()
    first = str(row.get('First Name', '')).strip().lower()
    # "Last, First" or just "Last"
    if ',' in q:
        parts = [p.strip() for p in q.split(',', 1)]
        if parts[0] == last and (not parts[1] or parts[1] == first):
            return True
    else:
        if q == last:
            return True
    return False


def run_late_adjust(late_files, lecture_files, output_dir='.', weights_csv=None, student=None):
    """
    Interactively review late submission penalties and optionally override scores.

    For each student in each late report the user is shown the original score,
    applied (penalized) score, and how late the submission was.  They can:
      Enter   keep the applied (penalized) score
      o       restore the original (unpenalized) score
      <num>   set an arbitrary score
      s       skip — leave the gradebook unchanged for this student
    """
    # Load lecture gradebooks
    lecture_dfs = {}
    lecture_paths = {}
    for lf in lecture_files:
        lp = Path(lf)
        if not lp.exists():
            print(f"ERROR: Gradebook not found: {lf}")
            sys.exit(1)
        lecture_dfs[lp.name] = read_csv_with_trailing_comma_fix(lp)
        lecture_paths[lp.name] = lp

    # (lec_name, row_idx, col_name, new_score)
    adjustments = []

    for late_file in late_files:
        lp = Path(late_file)
        if not lp.exists():
            print(f"ERROR: Late report not found: {late_file}")
            continue

        assignment_name = _assignment_name_from_late_file(lp.name)
        late_df = pd.read_csv(lp, encoding='utf-8-sig')

        if student:
            late_df = late_df[late_df.apply(_matches_student, axis=1, query=student)]

        if late_df.empty:
            continue

        print(f"\n{'='*60}")
        print(f"Assignment: {assignment_name}  ({len(late_df)} student(s))")
        print(f"{'='*60}")

        rows = list(late_df.iterrows())
        # adj_checkpoint[i] = len(adjustments) before student i was processed,
        # so we can roll back when the user goes back.
        adj_checkpoint = []
        i = 0
        while i < len(rows):
            _, row = rows[i]

            last          = str(row.get('Last Name', '')).strip()
            first         = str(row.get('First Name', '')).strip()
            student_id    = normalize_student_id(row.get('Student ID', ''))
            lab_section   = str(row.get('Lab Section', '')).strip()
            email         = str(row.get('School Email', '')).strip()
            score_date    = str(row.get('Score Date', '')).strip()
            how_late      = str(row.get('How Late', '')).strip()
            original_score = row.get('Original Score', '')
            applied_score  = row.get('Applied Score', '')
            penalty_factor = row.get('Penalty Factor', '')

            try:
                original_val = float(original_score)
            except (ValueError, TypeError):
                original_val = None
            try:
                applied_val = float(applied_score)
            except (ValueError, TypeError):
                applied_val = original_val
            try:
                penalty_pct = round((1.0 - float(penalty_factor)) * 100)
            except (ValueError, TypeError):
                penalty_pct = 0

            # Record checkpoint before prompting
            if len(adj_checkpoint) <= i:
                adj_checkpoint.append(len(adjustments))

            section_str = f"  |  Lab {lab_section}" if lab_section else ""
            print(f"\n  [{i+1}/{len(rows)}]  {last}, {first}  |  {student_id}{section_str}  |  {email}")
            print(f"  Submitted: {score_date}  |  Late: {how_late}")
            if original_val is not None and applied_val is not None:
                print(f"  Original: {original_val:.2f}  |  "
                      f"Penalty: {penalty_pct}%  |  Applied: {applied_val:.2f}")
            else:
                print(f"  Original: {original_score}  |  Applied: {applied_score}")

            default_display = f"{applied_val:.2f}" if applied_val is not None else "?"
            orig_display    = f"{original_val:.2f}" if original_val is not None else "?"
            back_hint = "  |  b = go back" if i > 0 else ""
            print(f"  [Enter] keep {default_display}  |  o = original ({orig_display})"
                  f"  |  <number> = custom  |  s = skip{back_hint}")

            while True:
                try:
                    choice = input("  > ").strip()
                except (EOFError, KeyboardInterrupt):
                    print("\nAborted.")
                    sys.exit(0)

                if choice.lower() == 'b':
                    if i > 0:
                        i -= 1
                        # Roll back any adjustment recorded for the student we're
                        # returning to (in case they had already confirmed one).
                        del adjustments[adj_checkpoint[i]:]
                        del adj_checkpoint[i]
                        print("  (going back)")
                    break
                elif choice == '':
                    new_score = applied_val
                    break
                elif choice.lower() == 'o':
                    new_score = original_val
                    break
                elif choice.lower() == 's':
                    new_score = None
                    break
                else:
                    try:
                        new_score = float(choice)
                        break
                    except ValueError:
                        print("  Invalid — enter a number, 'o', 's', 'b', or press Enter.")

            if choice.lower() == 'b':
                continue  # outer while re-displays the student at i

            if new_score is None:
                print("  Skipped.")
                i += 1
                continue

            # Locate student in the gradebooks
            found = False
            for lec_name, lec_df in lecture_dfs.items():
                try:
                    col = resolve_column(lec_df, assignment_name)
                except ValueError:
                    continue
                row_idx = _find_student_row(lec_df, student_id, email)
                if row_idx is not None:
                    adjustments.append((lec_name, row_idx, col, new_score))
                    found = True
                    break

            if not found:
                print(f"  WARNING: {last}, {first} not found in any gradebook.")
            else:
                print(f"  -> {new_score:.2f}")
            i += 1

    if not adjustments:
        print("\nNo adjustments to apply.")
        return

    # Apply all adjustments
    for lec_name, row_idx, col, score in adjustments:
        df = lecture_dfs[lec_name]
        df[col] = df[col].astype(object)
        df.at[row_idx, col] = f"{score:.2f}"

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    print("\nWriting updated gradebooks:")
    for lec_name, df in lecture_dfs.items():
        unnamed = [c for c in df.columns if 'Unnamed' in str(c)]
        if unnamed:
            df = df.drop(columns=unnamed)
        df = sort_assignment_columns(df)
        id_col = find_student_id_column(df)
        if id_col:
            df[id_col] = df[id_col].apply(normalize_student_id)
        recompute_averages(df, weights=load_weights_csv(weights_csv) if weights_csv else None)
        output_path = out / lecture_paths[lec_name].name
        df.to_csv(output_path, index=False, encoding='utf-8-sig', quoting=csv.QUOTE_ALL)
        print(f"   {output_path}")

    print("\nDone!")
