"""CLI entry point for pyzy - zyBooks grade processing tools."""

import argparse
import sys
from pathlib import Path


def build_parser():
    parser = argparse.ArgumentParser(
        prog='pyzy',
        description='zyBooks grade processing tools',
    )
    subparsers = parser.add_subparsers(dest='command', required=True)

    # --- assignment subcommand ---
    assignment_parser = subparsers.add_parser(
        'assignment',
        help='Process zyBooks assignment reports, apply late penalties, and update gradebooks',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Example:
    pyzy assignment report.csv
    pyzy assignment reports/ --due-dates-csv DueDates.csv --penalty 0.2 -l lecA.csv lecB.csv
    pyzy assignment W3_PA_deadlined.csv --lifted W3_PA_lifted.csv --due-dates-csv DueDates.csv --penalty 0.5 -l lec*.csv
""",
    )
    assignment_parser.add_argument(
        'input', nargs='?', default=None,
        metavar='FILE_OR_DIR',
        help='zyBooks assignment report CSV or directory. In two-report mode (--lifted), '
             'this is the deadlined report (pulled with grace-adjusted deadline active). '
             'Required unless --revert is used.',
    )
    assignment_parser.add_argument(
        '--lecture', '-l', nargs='+', default=None,
        help='BBLearn gradebook CSVs to update with scored grades (optional)',
    )
    assignment_parser.add_argument(
        '--output-dir', '-o', default='.',
        help='Output directory for updated gradebooks (default: current directory)',
    )
    assignment_parser.add_argument(
        '--due-dates-csv', default=None,
        metavar='FILE',
        help='CSV of due dates: one row per week, columns PA/CA/OL/IL60/IL61/... '
             'Naive datetimes are treated as America/New_York.',
    )
    assignment_parser.add_argument(
        '--weights-csv', default=None,
        metavar='FILE',
        help='CSV of per-assignment weights for WAVG computation: same structure as '
             '--due-dates-csv but values are fractional weights.',
    )
    assignment_parser.add_argument(
        '--days-grace', type=int, default=0,
        metavar='N',
        help='Days after due date before late penalty applies (default: 0)',
    )
    assignment_parser.add_argument(
        '--hours-grace', type=int, default=0,
        metavar='N',
        help='Additional hours after due date before late penalty applies (default: 0)',
    )
    assignment_parser.add_argument(
        '--penalty', type=float, default=0.2,
        metavar='F',
        help='Flat fraction deducted for any late submission beyond grace period (default: 0.2)',
    )
    assignment_parser.add_argument(
        '--date-audit', action='store_true',
        help='Write a per-assignment CSV showing every student\'s score date vs due date',
    )
    assignment_parser.add_argument(
        '--force', action='store_true',
        help='Overwrite gradebook scores even if the new score is lower than the existing one',
    )
    assignment_parser.add_argument(
        '--revert', default=None,
        metavar='PATTERN',
        help='Instead of scoring, copy this column substring from --original '
             'gradebooks back into the --lecture (working) gradebooks',
    )
    assignment_parser.add_argument(
        '--original', nargs='+', default=None,
        metavar='FILE',
        help='Original (unmodified) BBLearn gradebook CSVs used with --revert',
    )
    assignment_parser.add_argument(
        '--no-penalty', nargs='+', default=None,
        metavar='STUDENT_ID',
        help='Student ID(s) exempt from late penalties (score kept as-is regardless of lateness)',
    )
    assignment_parser.add_argument(
        '--best-one-of', action='store_true',
        help='Replace Percent score with the best single component column percentage '
             '(component columns match <int>.<int>, e.g. "19.1 - Lab (10)")',
    )
    assignment_parser.add_argument(
        '--due', default=None,
        metavar='DATETIME',
        help='Fallback due date/time used when --due-dates-csv has no entry for the assignment '
             '(or as the sole due date when --due-dates-csv is not provided). '
             'Naive datetimes are treated as America/New_York.',
    )
    assignment_parser.add_argument(
        '--name', '-n', default=None,
        metavar='NAME',
        help='Override the derived assignment name used as the gradebook column target '
             '(e.g. "W3 PA"). Only applies when processing a single file.',
    )
    assignment_parser.add_argument(
        '--aliases', default=None,
        metavar='FILE',
        help='CSV mapping zyBooks usernames to DrexelOne usernames. '
             'Columns: zybooks_username, username  (student_id optional).',
    )
    assignment_parser.add_argument(
        '--lifted', default=None,
        metavar='PATH',
        help='Lifted zyBooks report (file or directory) for two-report mode. '
             'When provided, the positional input is treated as the deadlined report and '
             'lateness is determined by score comparison: a student is late only if their '
             'lifted score exceeds their deadlined score. '
             '--days-grace and --hours-grace are ignored (grace is baked into the deadlined report).',
    )
    assignment_parser.add_argument(
        '--quiet', '-q', action='store_true',
        help='Suppress verbose output',
    )
    assignment_parser.add_argument(
        '--audit-log', default='pyzy_audit',
        metavar='DIR',
        help='Directory for per-assignment audit JSON files (default: pyzy_audit/). '
             'Pass an empty string to disable logging.',
    )

    # --- merge subcommand (merge_grades_v2) ---
    merge_parser = subparsers.add_parser(
        'merge',
        help='Transfer grades from per-assignment CSVs into lecture section gradebooks',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Example:
    pyzy merge --lecture section_A.csv section_B.csv --assignment W1_PA.csv W1_CA.csv
    pyzy merge -l sec_A.csv -a assignments/*.csv --output-dir merged/
    pyzy merge -l sec_A.csv --assignment-dir scored/ --assignment-pattern "*_bblearn.csv"
""",
    )
    merge_parser.add_argument(
        '--lecture', '-l', nargs='+', required=True,
        help='Lecture section CSV files',
    )
    merge_parser.add_argument(
        '--assignment', '-a', nargs='+',
        help='Per-assignment CSV files from zyBooks',
    )
    merge_parser.add_argument(
        '--assignment-dir', '-ad',
        help='Directory containing per-assignment CSV files (alternative to --assignment)',
    )
    merge_parser.add_argument(
        '--assignment-pattern', '-ap', default='*_bblearn.csv',
        help='Filename pattern for assignment CSVs in directory (default: "*_bblearn.csv")',
    )
    merge_parser.add_argument(
        '--output-dir', '-o', default='.',
        help='Output directory for merged files (default: current directory)',
    )
    merge_parser.add_argument(
        '--quiet', '-q', action='store_true',
        help='Suppress verbose output',
    )
    merge_parser.add_argument(
        '--weights-csv', default=None,
        metavar='FILE',
        help='CSV of per-assignment weights for WAVG computation.',
    )

    # --- activity subcommand ---
    activity_parser = subparsers.add_parser(
        'activity',
        help='Merge zyBooks activity report scores into lecture section gradebooks',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Example:
    pyzy activity report.csv -l section_A.csv section_B.csv -n "W1 CA"
    pyzy activity report.csv -l section_A.csv -n "W1 CA" --select recent --due "2026-03-09T23:59:00Z"
    pyzy activity a1.csv a2.csv -l gradebooks/*.csv -n "W1 CA" "W2 PA"
    pyzy activity a*.csv -l gradebooks/*.csv -n "CA"  (aggregate: best score + submission count)

The -n/--name values are substrings matched against existing gradebook columns.
Each substring must match exactly one column (error if ambiguous or not found).

Selection (--select): choose which attempt per student counts.
    max      highest raw score overall, then late penalty applied (default)
    recent   most recently submitted, then late penalty applied
    pre-due  best score among submissions on or before --due; no penalty;
             students with no on-time submission receive 0

Late penalty (--due): applied to the selected submission (max/recent only).
    0-24 h late: -20%  |  each additional 24 h: -10%
""",
    )
    activity_parser.add_argument(
        'input', nargs='+', help='Activity report CSV file(s)',
    )
    activity_parser.add_argument(
        '--lecture', '-l', nargs='+', required=True,
        help='Lecture section gradebook CSV files',
    )
    activity_parser.add_argument(
        '--name', '-n', nargs='+', required=True,
        help='Substring(s) identifying existing gradebook column(s): one per input file, '
             'or one substring to aggregate all into a single column',
    )
    activity_parser.add_argument(
        '--select', '-s', choices=['max', 'recent', 'pre-due'], default='max',
        help='Which submission per student counts: '
             '"max" (highest raw score, then penalty applied, default); '
             '"recent" (most recently submitted, then penalty applied); '
             '"pre-due" (best score among on-time submissions only — requires --due; '
             'students with no on-time submission receive 0)',
    )
    activity_parser.add_argument(
        '--due', '-D', default=None,
        metavar='DATETIME',
        help='Due date/time (ISO 8601, e.g. "2026-03-09T23:59:00Z"). '
             'Late submissions receive: -20%% for any lateness, then -10%% per additional 24 h.',
    )
    activity_parser.add_argument(
        '--output-dir', '-o', default='.',
        help='Output directory for updated gradebooks (default: current directory)',
    )
    activity_parser.add_argument(
        '--quiet', '-q', action='store_true',
        help='Suppress verbose output',
    )
    activity_parser.add_argument(
        '--force', action='store_true',
        help='Overwrite gradebook scores even if the new score is lower than the existing one',
    )
    activity_parser.add_argument(
        '--days-grace', type=int, default=0,
        metavar='N',
        help='Days after due date before late penalty applies (default: 0)',
    )
    activity_parser.add_argument(
        '--hours-grace', type=int, default=0,
        metavar='N',
        help='Additional hours after due date before late penalty applies (default: 0)',
    )
    activity_parser.add_argument(
        '--grace-limit', type=float, default=None,
        metavar='DAYS',
        help='Zero out scores for submissions more than this many days late (default: no limit)',
    )
    activity_parser.add_argument(
        '--penalty', type=float, default=0.2,
        metavar='FRAC',
        help='Flat fraction deducted for any late submission beyond grace period (default: 0.2)',
    )
    activity_parser.add_argument(
        '--weights-csv', default=None,
        metavar='FILE',
        help='CSV of per-assignment weights for WAVG computation.',
    )
    activity_parser.add_argument(
        '--no-penalty', nargs='+', default=None,
        metavar='STUDENT_ID',
        help='Student ID(s) exempt from late penalties (looked up via gradebook)',
    )
    activity_parser.add_argument(
        '--aliases', default=None,
        metavar='FILE',
        help='CSV mapping zyBooks usernames to DrexelOne usernames. '
             'Columns: zybooks_username, username  (student_id optional).',
    )
    activity_parser.add_argument(
        '--audit-log', default='pyzy_audit',
        metavar='DIR',
        help='Directory for per-assignment audit JSON files (default: pyzy_audit/). '
             'Pass an empty string to disable logging.',
    )

    # --- late-adjust subcommand ---
    la_parser = subparsers.add_parser(
        'late-adjust',
        help='Interactively review and override late-penalty scores from assignment reports',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Example:
    pyzy late-adjust --late out/W3_PA_late.csv out/W5_IL_late.csv -l lecA.csv lecB.csv
    pyzy late-adjust --late-dir out/ -l lecA.csv lecB.csv -o adjusted/

For each student in each late report you will be prompted:
  Enter        keep the applied (penalized) score
  o            restore the original (unpenalized) score
  <number>     set a custom score
  s            skip — leave the gradebook unchanged
""",
    )
    la_parser.add_argument(
        '--late', '-r', nargs='+', default=None,
        metavar='FILE',
        help='Late report CSV file(s) produced by the assignment subcommand',
    )
    la_parser.add_argument(
        '--late-dir', default=None,
        metavar='DIR',
        help='Directory containing *_late.csv files (alternative to --late)',
    )
    la_parser.add_argument(
        '--lecture', '-l', nargs='+', required=True,
        help='BBLearn gradebook CSVs to update',
    )
    la_parser.add_argument(
        '--output-dir', '-o', default='.',
        help='Output directory for updated gradebooks (default: current directory)',
    )
    la_parser.add_argument(
        '--weights-csv', default=None,
        metavar='FILE',
        help='CSV of per-assignment weights for WAVG computation.',
    )
    la_parser.add_argument(
        '--student', default=None,
        metavar='QUERY',
        help='Filter to one student: accepts student ID, username, or "Last, First" name',
    )

    # --- late-report subcommand ---
    lr_parser = subparsers.add_parser(
        'late-report',
        help='Pivot multiple *_late.csv files into a single per-student lateness summary',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Example:
    pyzy late-report --late out/W3_PA_late.csv out/W5_IL_late.csv -o late_summary.csv
    pyzy late-report --late-dir out/ -o late_summary.csv
""",
    )
    lr_parser.add_argument(
        '--late', '-r', nargs='+', default=None,
        metavar='FILE',
        help='Late report CSV file(s) produced by the assignment subcommand',
    )
    lr_parser.add_argument(
        '--late-dir', default=None,
        metavar='DIR',
        help='Directory containing *_late.csv files (alternative to --late)',
    )
    lr_parser.add_argument(
        '--output', '-o', default='late_summary.csv',
        metavar='FILE',
        help='Output CSV path (default: late_summary.csv)',
    )

    # --- query subcommand ---
    query_parser = subparsers.add_parser(
        'query',
        help='Look up a student\'s grades across one or more gradebooks',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Example:
    pyzy query -l lecA.csv lecB.csv --id 14788528
    pyzy query -l lecA.csv lecB.csv --last Smith --first John
    pyzy query -l lecA.csv lecB.csv --id 14788528 --column "W3 PA"
""",
    )
    query_parser.add_argument(
        '--lecture', '-l', nargs='+', required=True,
        help='BBLearn gradebook CSV files to search',
    )
    query_parser.add_argument(
        '--id', dest='student_id', default=None,
        metavar='STUDENT_ID',
        help='Student ID to look up',
    )
    query_parser.add_argument(
        '--last', default=None,
        metavar='NAME',
        help='Last name (use with --first)',
    )
    query_parser.add_argument(
        '--first', default=None,
        metavar='NAME',
        help='First name (use with --last)',
    )
    query_parser.add_argument(
        '--column', '-c', default=None,
        metavar='PATTERN',
        help='Substring to filter grade columns (shows all if omitted)',
    )

    # --- assign-lab-section subcommand ---
    als_parser = subparsers.add_parser(
        'assign-lab-section',
        help='Stamp lab section into lecture gradebooks from per-section gradebook files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Example:
    pyzy assign-lab-section -l lecA.csv lecB.csv -L lab60_gradebook.csv lab61_gradebook.csv
""",
    )
    als_parser.add_argument(
        '--lecture', '-l', nargs='+', required=True,
        help='Lecture section gradebook CSV files (updated in-place)',
    )
    als_parser.add_argument(
        '--lab', '-L', nargs='+', required=True,
        help='Lab-section gradebook CSV files; section number parsed from filename '
             '(e.g. lab60_gradebook.csv -> IL60)',
    )
    als_parser.add_argument(
        '--quiet', '-q', action='store_true',
        help='Suppress verbose output',
    )

    # --- log subcommand ---
    log_parser = subparsers.add_parser(
        'log',
        help='Query the audit log for a student\'s grade history',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Example:
    pyzy log --id 14788528
    pyzy log --username john.smith
    pyzy log --last Smith --first John
    pyzy log --id 14788528 --audit-log /path/to/pyzy_audit/
""",
    )
    log_parser.add_argument(
        '--audit-log', default='pyzy_audit',
        metavar='DIR',
        help='Directory containing per-assignment audit JSON files (default: pyzy_audit/)',
    )
    log_parser.add_argument(
        '--id', dest='student_id', default=None,
        metavar='STUDENT_ID',
        help='Student ID to look up',
    )
    log_parser.add_argument(
        '--username', '-u', default=None,
        metavar='USERNAME',
        help='Username (email prefix) to look up',
    )
    log_parser.add_argument(
        '--last', default=None,
        metavar='NAME',
        help='Last name to look up',
    )
    log_parser.add_argument(
        '--first', default=None,
        metavar='NAME',
        help='First name (use with --last)',
    )

    return parser


def main(argv=None):
    from .common import load_aliases_csv  # noqa: F401 (used in dispatch below)

    parser = build_parser()
    args = parser.parse_args(argv)

    def _make_audit_log(path_str):
        if not path_str:
            return None
        from .audit import AuditLog
        return AuditLog(path_str)

    if args.command == 'assignment':
        if args.revert:
            if not args.lecture or not args.original:
                print("ERROR: --revert requires both --lecture (working) and --original files")
                sys.exit(1)
            from .assignment import run_revert
            run_revert(
                pattern=args.revert,
                lecture_files=args.lecture,
                original_files=args.original,
                quiet=args.quiet,
                weights_csv=args.weights_csv,
            )
        elif not args.input:
            print("ERROR: input file/directory is required (or use --revert for undo mode)")
            sys.exit(1)
        else:
            from .assignment import run_assignment
            run_assignment(
                deadline_input=args.input,
                lecture_files=args.lecture,
                output_dir=args.output_dir,
                quiet=args.quiet,
                due_dates_csv=args.due_dates_csv,
                days_grace=args.days_grace,
                hours_grace=args.hours_grace,
                penalty=args.penalty,
                date_audit=args.date_audit,
                force=args.force,
                best_one_of=args.best_one_of,
                due=args.due,
                name=args.name,
                no_penalty_ids=args.no_penalty,
                weights_csv=args.weights_csv,
                audit_log=_make_audit_log(args.audit_log),
                deadlined_input=args.lifted,
                aliases=load_aliases_csv(args.aliases) if args.aliases else None,
            )
    elif args.command == 'merge':
        from .merge import run_merge
        run_merge(
            lecture_files=args.lecture,
            assignment_files=args.assignment,
            assignment_dir=args.assignment_dir,
            assignment_pattern=args.assignment_pattern,
            output_dir=args.output_dir,
            quiet=args.quiet,
            weights_csv=args.weights_csv,
        )
    elif args.command == 'activity':
        from .activity import run_activity
        run_activity(
            input_files=args.input,
            lecture_files=args.lecture,
            column_names=args.name,
            output_dir=args.output_dir,
            quiet=args.quiet,
            due_date=args.due,
            select=args.select,
            force=args.force,
            days_grace=args.days_grace,
            hours_grace=args.hours_grace,
            grace_limit=args.grace_limit,
            penalty=args.penalty,
            weights_csv=args.weights_csv,
            no_penalty_ids=args.no_penalty,
            audit_log=_make_audit_log(args.audit_log),
            aliases=load_aliases_csv(args.aliases) if args.aliases else None,
        )
    elif args.command == 'late-adjust':
        late_files = args.late or []
        if args.late_dir:
            d = Path(args.late_dir)
            if not d.is_dir():
                print(f"ERROR: Not a directory: {args.late_dir}")
                sys.exit(1)
            late_files = late_files + sorted(str(p) for p in d.glob('*_late.csv'))
        if not late_files:
            print("ERROR: No late report files found. Use --late or --late-dir.")
            sys.exit(1)
        from .late_adjust import run_late_adjust
        run_late_adjust(
            late_files=late_files,
            lecture_files=args.lecture,
            output_dir=args.output_dir,
            weights_csv=args.weights_csv,
            student=args.student,
        )
    elif args.command == 'late-report':
        from .late_report import run_late_report
        run_late_report(
            late_files=args.late,
            late_dir=args.late_dir,
            output_path=args.output,
        )
    elif args.command == 'query':
        from .query import run_query
        run_query(
            lecture_files=args.lecture,
            student_id=args.student_id,
            last=args.last,
            first=args.first,
            column_pattern=args.column,
        )
    elif args.command == 'assign-lab-section':
        from .labsection import run_assign_lab_section
        run_assign_lab_section(
            lecture_files=args.lecture,
            lab_files=args.lab,
            quiet=args.quiet,
        )
    elif args.command == 'log':
        if not any([args.student_id, args.username, args.last]):
            print("ERROR: Provide at least one of --id, --username, or --last")
            sys.exit(1)
        from .log_cmd import run_log
        run_log(
            audit_dir=args.audit_log,
            student_id=args.student_id,
            username=args.username,
            last=args.last,
            first=args.first,
        )


if __name__ == '__main__':
    main()
