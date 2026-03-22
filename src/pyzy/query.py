"""
Query subcommand - look up a student's grades across one or more gradebooks,
merging columns from different sources by assignment short-name.
"""

import re
import sys
from pathlib import Path

import pandas as pd

from .common import (
    find_email_column,
    find_name_columns,
    find_student_id_column,
    normalize_student_id,
    read_csv_with_trailing_comma_fix,
)
from .merge import find_username_column


_IDENTITY_PATTERNS = [
    'student id', 'studentid', 'student_id', 'sid', 'id',
    'student number', 'student_number',
    'first name', 'last name', 'firstname', 'lastname',
    'email', 'username', 'user name',
    'class section', 'section',
]

_ASSIGNMENT_TYPES = ('PA', 'CA', 'IL', 'OL')
_TYPE_ORDER = {t: i for i, t in enumerate(_ASSIGNMENT_TYPES)}

# Plain column names that are never assignment scores — go to the header section.
_NON_ASSIGNMENT_COLS = {
    'availability', 'last access', 'weighted total', 'points possible',
    'how late', 'score date', 'original score', 'applied score',
    'penalty factor', 'lab section', 'number submitted',
}


def _is_identity_col(col):
    cl = col.lower().replace(' ', '').replace('_', '').replace('-', '')
    for pat in _IDENTITY_PATTERNS:
        if pat.replace(' ', '').replace('_', '') in cl:
            return True
    return False


def _short_name(col):
    """
    Extract a canonical short assignment name from a column header.

    'W3 PA [Total Pts: 100 Score] |3749455'  ->  'W3 PA'
    'W3 PA'                                   ->  'W3 PA'
    'PP1'                                     ->  'PP1'  (plain assignment name)
    'PA AVG [...]', 'Availability [...]'      ->  None   (verbose non-assignment)
    'How Late', 'Last Access', 'PA AVG'       ->  None   (known non-assignment plain)
    Returns None for anything that should not appear in the assignment table.
    """
    col = col.strip()
    # W<n> TYPE  (with optional BBLearn verbose suffix)
    m = re.match(r'(W\d+\s+(?:PA|CA|IL|OL))\b', col, re.IGNORECASE)
    if m:
        return m.group(1).upper()
    # BBLearn verbose column — extract prefix before '[' or '|'
    if '[' in col or '|' in col:
        prefix = re.split(r'\s*[\[|]', col)[0].strip()
        # Assignment code: one or more letters followed by one or more digits (PP1, HW2, Quiz1)
        if re.match(r'^[A-Za-z]+\d+$', prefix):
            return prefix
        return None
    # TYPE AVG (plain or verbose) — not an assignment score
    if re.match(r'(?:PA|CA|IL|OL)\s+AVG\b', col, re.IGNORECASE):
        return None
    # Known non-assignment plain columns
    if col.lower() in _NON_ASSIGNMENT_COLS:
        return None
    # Plain assignment name (e.g. 'PP1', 'W3 PA' already caught above)
    return col


def _assignment_sort_key(name):
    """Sort key for short names: week asc, then PA/CA/IL/OL, then AVG last."""
    m = re.match(r'W(\d+)\s+(\S+)', name)
    if m:
        return (int(m.group(1)), _TYPE_ORDER.get(m.group(2).upper(), 99), 0)
    m = re.match(r'(PA|CA|IL|OL)\s+AVG', name)
    if m:
        return (9999, _TYPE_ORDER.get(m.group(1).upper(), 99), 1)
    return (99999, 99, 0)


def _find_student(df, student_id=None, last=None, first=None):
    """Return the row index of the matching student, or None."""
    id_col = find_student_id_column(df)
    first_col, last_col = find_name_columns(df)

    if student_id and id_col:
        for idx, row in df.iterrows():
            if normalize_student_id(row[id_col]) == student_id:
                return idx

    if last and first and first_col and last_col:
        last_l  = last.strip().lower()
        first_l = first.strip().lower()
        for idx, row in df.iterrows():
            row_last  = str(row[last_col]).strip().lower() if pd.notna(row[last_col])  else ''
            row_first = str(row[first_col]).strip().lower() if pd.notna(row[first_col]) else ''
            if row_last == last_l and row_first == first_l:
                return idx

    return None


def run_query(lecture_files, student_id=None, last=None, first=None,
              column_pattern=None):
    """
    Find a student across one or more gradebooks and print a unified table.

    Assignment columns from different files are merged by short name
    (e.g. 'W3 PA [Total Pts: 100] |...' and 'W3 PA' both map to 'W3 PA'),
    so scores from a BBLearn gradebook and lateness from a late-summary CSV
    appear on the same row.

    Non-assignment columns (lab section, etc.) appear in the per-file header.
    """
    if not student_id and not (last and first):
        print("ERROR: Provide --id or both --last and --first.")
        sys.exit(1)

    # ------------------------------------------------------------------ collect
    # sources: list of (label, {short_name: value_str}, identity_dict, misc_cols)
    # misc_cols: [(col_header, value)] for non-assignment, non-identity columns
    sources = []
    identity_fields = {}   # merged across all files

    for lf in lecture_files:
        lp = Path(lf)
        if not lp.exists():
            print(f"ERROR: File not found: {lf}")
            continue

        df = read_csv_with_trailing_comma_fix(lp)
        idx = _find_student(df, student_id=student_id, last=last, first=first)
        if idx is None:
            continue

        row = df.loc[idx]

        # identity
        id_col    = find_student_id_column(df)
        email_col = find_email_column(df)
        un_col    = find_username_column(df)
        first_col, last_col = find_name_columns(df)

        if last_col and first_col and 'name' not in identity_fields:
            identity_fields['name'] = (
                f"{row.get(last_col, '')}, {row.get(first_col, '')}"
            )
        if id_col and 'id' not in identity_fields:
            identity_fields['id'] = normalize_student_id(row[id_col]) or ''
        if email_col and 'email' not in identity_fields:
            identity_fields['email'] = str(row[email_col]).strip()
        if un_col and 'username' not in identity_fields:
            identity_fields['username'] = str(row[un_col]).strip()

        # grade and misc columns
        assign_vals = {}  # short_name -> value_str
        misc_cols   = []  # (col, value_str) for non-assignment, non-identity

        for col in df.columns:
            if 'Unnamed' in str(col) or _is_identity_col(col):
                continue
            val = row.get(col, '')
            val_str = '' if pd.isna(val) else str(val).strip()

            sname = _short_name(col)
            if sname:
                if column_pattern is None or column_pattern.lower() in sname.lower():
                    assign_vals[sname] = val_str
            else:
                if column_pattern is None or column_pattern.lower() in col.lower():
                    misc_cols.append((col, val_str))

        if assign_vals or misc_cols:
            sources.append((lp.name, assign_vals, misc_cols))

    if not sources:
        desc = f"ID {student_id}" if student_id else f"{last}, {first}"
        print(f"\nStudent not found: {desc}")
        return

    # ------------------------------------------------------------------ header
    print(f"\n{'='*60}")
    if identity_fields.get('name'):
        print(f"Name      : {identity_fields['name']}")
    if identity_fields.get('id'):
        print(f"Student ID: {identity_fields['id']}")
    if identity_fields.get('email'):
        print(f"Email     : {identity_fields['email']}")
    if identity_fields.get('username'):
        u = identity_fields['username']
        if u and u != identity_fields.get('email', ''):
            print(f"Username  : {u}")

    # per-source misc columns (lab section, etc.)
    for label, _, misc_cols in sources:
        for col, val in misc_cols:
            print(f"{col} ({label}): {val}")
    print(f"{'='*60}")

    # ------------------------------------------------------------------ table
    # Collect all short names across all sources
    all_names = sorted(
        {name for _, av, _ in sources for name in av},
        key=_assignment_sort_key,
    )

    if not all_names:
        print("  (no assignment columns found)")
        return

    source_labels = [label for label, _, _ in sources]
    source_vals   = [av    for _, av, _ in sources]

    # Per-source column width: wide enough for the label and all its values
    name_w = max(len(n) for n in all_names) + 4
    col_ws = [
        max(len(label), max((len(av.get(n, '')) for n in all_names), default=0), 1)
        for label, av in zip(source_labels, source_vals)
    ]

    sep = "  "
    header = f"  {'Assignment':<{name_w}}{sep}" + sep.join(
        f"{l:<{w}}" for l, w in zip(source_labels, col_ws)
    )
    divider = f"  {'-'*name_w}{sep}" + sep.join('-' * w for w in col_ws)
    print(header)
    print(divider)

    for name in all_names:
        vals = [av.get(name, '') for av in source_vals]
        row_str = f"  {name:<{name_w}}{sep}" + sep.join(
            f"{v:<{w}}" for v, w in zip(vals, col_ws)
        )
        print(row_str)
