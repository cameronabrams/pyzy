"""
Log query command — prints a per-student grade history from the audit directory.
"""

import sys
from pathlib import Path

from .audit import AuditLog


def run_log(audit_dir, student_id=None, username=None, last=None, first=None):
    """
    Query the audit log directory for a student and print a summary table.

    Args:
        audit_dir:  Path to the directory containing per-assignment JSON audit files
        student_id: Student ID to search for
        username:   Username (email prefix) to search for
        last:       Last name to search for
        first:      First name (requires last)
    """
    directory = Path(audit_dir)
    if not directory.exists():
        print(f"ERROR: Audit log directory not found: {directory}")
        sys.exit(1)

    log = AuditLog(directory)
    log.load_all()
    results = log.query_student(
        student_id=student_id, username=username, last=last, first=first
    )

    if not results:
        print("No records found.")
        return

    # Identify the student from the first result
    _, first_rec = results[0]
    sid = first_rec.get('student_id') or '?'
    un = first_rec.get('username') or '?'
    name = first_rec.get('name') or '?'
    print(f"\nStudent: {sid}  ({un})  —  {name}\n")

    # Build rows: sort by (assignment, timestamp) so history is readable
    rows = []
    for run, rec in sorted(results, key=lambda x: (x[0]['assignment'], x[0]['timestamp'])):
        raw = rec.get('raw_score')
        pf = rec.get('penalty_factor')
        final = rec.get('final_score')
        rows.append([
            run['assignment'],
            run['command'],
            run['timestamp'][:10],
            f"{raw:.2f}" if raw is not None else '—',
            f"{pf:.2f}" if pf is not None else '—',
            f"{final:.2f}" if final is not None else '—',
            rec.get('status') or '—',
            rec.get('how_late') or '',
        ])

    headers = ['Assignment', 'Command', 'Date', 'Raw%', 'Penalty', 'Final%', 'Status', 'How Late']
    widths = [
        max(len(headers[i]), max(len(str(r[i])) for r in rows))
        for i in range(len(headers))
    ]
    fmt = '  '.join(f'{{:<{w}}}' for w in widths)
    rule = '  '.join('-' * w for w in widths)

    print(fmt.format(*headers))
    print(rule)
    for row in rows:
        print(fmt.format(*[str(x) for x in row]))
    print()
