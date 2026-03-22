"""
Audit log — persistent JSON record of every grade written by pyzy.

Each call to `pyzy assignment` or `pyzy activity` writes a JSON file named
after the assignment (e.g. PP1.json, W1_CA.json) into the audit directory,
overwriting any previous run for that assignment.  The `pyzy log` subcommand
loads all files in the directory to produce a per-student grade history.
"""

import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path

_SCHEMA_VERSION = 1
_SAFE_RE = re.compile(r'[^\w\- ]')


def _safe_filename(name):
    """Convert an assignment name to a safe filename stem."""
    return _SAFE_RE.sub('', name).replace(' ', '_')


def _sanitize(rec):
    """Replace float NaN/Inf with None so records are valid JSON."""
    return {
        k: None if (isinstance(v, float) and not math.isfinite(v)) else v
        for k, v in rec.items()
    }


def _now_iso():
    return datetime.now(tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


class AuditLog:
    """
    Per-assignment audit log stored as one JSON file per assignment.

    Usage (writing)::

        log = AuditLog('audit_logs/')
        log.append_run(
            command='assignment',
            assignment='W3 PA',
            lecture_files=['section_A.csv'],
            args={'due_date': '...', 'penalty': 0.2},
            records=[
                {'student_id': '12345', 'username': 'john.smith',
                 'name': 'Smith, John', 'raw_score': 95.5,
                 'penalty_factor': 0.8, 'final_score': 76.4,
                 'status': 'late', 'score_date': '2026-03-11T02:15:00-05:00',
                 'how_late': '1d 3h'},
            ],
        )
        log.save()   # writes audit_logs/W3_PA.json, overwriting any previous run

    Usage (querying)::

        log = AuditLog('audit_logs/')
        log.load_all()
        results = log.query_student(last='Smith', first='john')
    """

    def __init__(self, directory):
        self.directory = Path(directory)
        self.runs = []

    def append_run(self, command, assignment, lecture_files, records, args=None):
        """
        Append a run entry to the log (in memory; call save() to persist).

        Args:
            command:       'assignment' or 'activity'
            assignment:    column name / assignment label (e.g. 'W3 PA', 'PP1')
            lecture_files: list of lecture gradebook filenames (basenames)
            records:       list of per-student grade record dicts
            args:          dict of relevant CLI arguments for this run
        """
        if not records:
            return
        run_id = (self.runs[-1]['run_id'] + 1) if self.runs else 1
        self.runs.append({
            'run_id': run_id,
            'timestamp': _now_iso(),
            'command': command,
            'assignment': assignment,
            'lecture_files': [str(lf) for lf in (lecture_files or [])],
            'args': args or {},
            'records': [_sanitize(r) for r in records],
        })

    def save(self):
        """
        Write one JSON file per assignment into self.directory, overwriting
        any previous file for that assignment.
        """
        self.directory.mkdir(parents=True, exist_ok=True)
        by_assignment = {}
        for run in self.runs:
            by_assignment.setdefault(run['assignment'], []).append(run)
        for aname, runs in by_assignment.items():
            path = self.directory / f'{_safe_filename(aname)}.json'
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(
                    {'version': _SCHEMA_VERSION, 'assignment': aname, 'runs': runs},
                    f, indent=2,
                )

    def load_all(self):
        """Load all run records from the directory into self.runs for querying."""
        self.runs = []
        if not self.directory.exists():
            return
        for jf in sorted(self.directory.glob('*.json')):
            try:
                with open(jf, encoding='utf-8') as f:
                    data = json.load(f)
                self.runs.extend(data.get('runs', []))
            except (json.JSONDecodeError, OSError):
                pass

    def query_student(self, student_id=None, username=None, last=None, first=None):
        """
        Return all (run, record) pairs that match the given student.

        Matching is tried in order: student_id, username, last+first name.
        Multiple criteria can match the same record (all are OR-combined).
        Call load_all() before querying if you want to search the full directory.
        """
        if not any([student_id, username, last]):
            return []

        norm_id = student_id.strip() if student_id else None
        norm_un = username.strip().lower() if username else None
        norm_last = last.strip().lower() if last else None
        norm_first = first.strip().lower() if first else None

        results = []
        for run in self.runs:
            for rec in run.get('records', []):
                match = False
                if norm_id and rec.get('student_id') == norm_id:
                    match = True
                if norm_un and rec.get('username', '').lower() == norm_un:
                    match = True
                if norm_last:
                    name = rec.get('name', '')
                    parts = [p.strip().lower() for p in name.split(',')]
                    if parts and parts[0] == norm_last:
                        if norm_first is None or (len(parts) > 1 and parts[1] == norm_first):
                            match = True
                if match:
                    results.append((run, rec))
        return results
