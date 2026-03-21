"""CLI entry point for pyzy - zyBooks grade processing tools."""

import argparse
import sys


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
    pyzy assignment -d report.csv
    pyzy assignment -d reports/ --due-dates-csv DueDates.csv --days-grace 1 --penalty 0.2 -l lecA.csv lecB.csv
""",
    )
    assignment_parser.add_argument(
        '--deadline', '-d', default=None,
        metavar='FILE_OR_DIR',
        help='zyBooks assignment report CSV, or a directory of them '
             '(required unless --revert is used)',
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
        '--quiet', '-q', action='store_true',
        help='Suppress verbose output',
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

Selection (--select): choose which attempt per student counts before any penalty.
    max     highest raw score (default)
    recent  most recently submitted

Late penalty (--due): applied to the selected submission.
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
        '--select', '-s', choices=['max', 'recent'], default='max',
        help='Which submission per student counts: "max" (highest raw score, default) '
             'or "recent" (most recently submitted)',
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

    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)

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
            )
        elif not args.deadline:
            print("ERROR: --deadline is required (or use --revert for undo mode)")
            sys.exit(1)
        else:
            from .assignment import run_assignment
            run_assignment(
                deadline_input=args.deadline,
                lecture_files=args.lecture,
                output_dir=args.output_dir,
                quiet=args.quiet,
                due_dates_csv=args.due_dates_csv,
                days_grace=args.days_grace,
                hours_grace=args.hours_grace,
                penalty=args.penalty,
                date_audit=args.date_audit,
                force=args.force,
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
        )
    elif args.command == 'assign-lab-section':
        from .labsection import run_assign_lab_section
        run_assign_lab_section(
            lecture_files=args.lecture,
            lab_files=args.lab,
            quiet=args.quiet,
        )


if __name__ == '__main__':
    main()
