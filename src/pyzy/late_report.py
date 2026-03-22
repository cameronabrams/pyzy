"""
late-report subcommand - pivots multiple *_late.csv files into a single
per-student summary with one column per assignment.
"""

import sys
from pathlib import Path

import pandas as pd

from .common import normalize_student_id


def _assignment_name_from_late_file(filename):
    stem = Path(filename).stem
    if stem.endswith('_late'):
        return stem[:-5].replace('_', ' ')
    return stem.replace('_', ' ')


def _student_key(row):
    """Return a stable key for a student row: prefer ID, fall back to email."""
    sid = normalize_student_id(row.get('Student ID', ''))
    if sid:
        return sid
    email = str(row.get('School Email', '')).strip().lower()
    return email or None


def run_late_report(late_files=None, output_path='late_summary.csv', late_dir=None):
    """
    Read one or more *_late.csv files and write a single CSV with one row per
    student and one column per assignment.  Students who were not late for a
    given assignment get "on time" in that cell.

    late_dir: if provided, globs all *_late.csv files from that directory
              (in addition to any explicit late_files).
    """
    late_files = list(late_files or [])
    if late_dir:
        d = Path(late_dir)
        if not d.is_dir():
            print(f"ERROR: Not a directory: {late_dir}")
            sys.exit(1)
        late_files = late_files + sorted(str(p) for p in d.glob('*_late.csv'))
    if not late_files:
        print("ERROR: No late report files found.")
        sys.exit(1)
    # identity fields preserved in output
    IDENTITY = ['Last Name', 'First Name', 'Student ID', 'School Email']

    assignment_names = []   # ordered list of assignment names
    # key -> {assignment_name: how_late}
    late_map   = {}
    # key -> identity dict
    identity_map = {}

    for lf in late_files:
        lp = Path(lf)
        if not lp.exists():
            print(f"ERROR: File not found: {lf}")
            sys.exit(1)

        aname = _assignment_name_from_late_file(lp.name)
        if aname not in assignment_names:
            assignment_names.append(aname)

        df = pd.read_csv(lp, encoding='utf-8-sig')
        if df.empty:
            continue

        for _, row in df.iterrows():
            key = _student_key(row)
            if key is None:
                continue

            if key not in identity_map:
                identity_map[key] = {f: str(row.get(f, '')).strip() for f in IDENTITY}

            how_late = str(row.get('How Late', '')).strip()
            late_map.setdefault(key, {})[aname] = how_late or 'late'

    if not identity_map:
        print("No students found across the provided late files.")
        return

    # Sort assignment names naturally (W1 PA, W2 CA, ...)
    def _sort_key(name):
        import re
        m = re.match(r'W(\d+)\s+(\S+)', name)
        type_order = {'PA': 0, 'CA': 1, 'IL': 2, 'OL': 3}
        if m:
            return (int(m.group(1)), type_order.get(m.group(2).upper(), 99))
        return (999, 0)

    assignment_names.sort(key=_sort_key)

    # Build output rows
    rows = []
    for key, identity in sorted(identity_map.items(),
                                 key=lambda kv: (kv[1].get('Last Name', ''),
                                                 kv[1].get('First Name', ''))):
        row = dict(identity)
        for aname in assignment_names:
            row[aname] = late_map.get(key, {}).get(aname, 'on time')
        rows.append(row)

    out_df = pd.DataFrame(rows, columns=IDENTITY + assignment_names)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out, index=False, encoding='utf-8-sig')

    print(f"Late report: {len(rows)} student(s)  x  {len(assignment_names)} assignment(s)")
    for aname in assignment_names:
        n_late = sum(1 for r in rows if r[aname] != 'on time')
        print(f"   {aname}: {n_late} late")
    print(f"\nWritten: {out}")
