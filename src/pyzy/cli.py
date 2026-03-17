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
        help='Score assignment CSVs, optionally merging deadline/lifted pairs and applying late penalties',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Example:
    pyzy assignment -d report.csv                              (single file, no modifications)
    pyzy assignment -d report.csv -l sec_A.csv sec_B.csv       (single file, update gradebooks)
    pyzy assignment -d deadline.csv -L lifted.csv -a adj.yaml -l sec_A.csv sec_B.csv
    pyzy assignment -d deadline/ -L lifted/ -a adj.yaml -l gradebooks/*.csv -o merged/
""",
    )
    assignment_parser.add_argument(
        '--deadline', '-d', required=True,
        metavar='FILE_OR_DIR',
        help='Assignment CSV with original due dates, or a directory of such CSVs',
    )
    assignment_parser.add_argument(
        '--lifted', '-L', default=None,
        metavar='FILE_OR_DIR',
        help='Assignment CSV with lifted deadlines, or a directory of such CSVs (optional). '
             'When omitted, scores are taken from --deadline as-is with no modifications. '
             'Must match the type (file or directory) of --deadline.',
    )
    assignment_parser.add_argument(
        '--lecture', '-l', nargs='+', default=None,
        help='Lecture section gradebook CSV files to update with scored grades (optional)',
    )
    assignment_parser.add_argument(
        '--output-dir', '-o', default='.',
        help='Output directory for output files (default: current directory)',
    )
    assignment_parser.add_argument(
        '--adjustments', '-a', default=None,
        help='YAML file containing score adjustments (optional; ignored without --lifted)',
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

    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == 'assignment':
        from .assignment import run_assignment
        run_assignment(
            deadline_input=args.deadline,
            lifted_input=args.lifted,
            lecture_files=args.lecture,
            output_dir=args.output_dir,
            adjustments_file=args.adjustments,
            quiet=args.quiet,
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
        )


if __name__ == '__main__':
    main()
