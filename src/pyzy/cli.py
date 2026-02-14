"""CLI entry point for pyzy - zyBooks grade processing tools."""

import argparse
import sys


def build_parser():
    parser = argparse.ArgumentParser(
        prog='pyzy',
        description='zyBooks grade processing tools',
    )
    subparsers = parser.add_subparsers(dest='command', required=True)

    # --- score subcommand (batch_merge_deadline_lift) ---
    score_parser = subparsers.add_parser(
        'score',
        help='Merge before/after deadline-lift assignment pairs and apply late penalties',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Example:
    pyzy score --deadline deadline/ --lifted lifted/
    pyzy score -d deadline/ -l lifted/ -o merged/ -a adjustments.yaml
""",
    )
    score_parser.add_argument(
        '--deadline', '-d', required=True,
        help='Directory containing assignment CSVs with original due dates',
    )
    score_parser.add_argument(
        '--lifted', '-l', required=True,
        help='Directory containing assignment CSVs with lifted deadlines',
    )
    score_parser.add_argument(
        '--output-dir', '-o', default='merged',
        help='Output directory for merged files (default: merged/)',
    )
    score_parser.add_argument(
        '--adjustments', '-a', default=None,
        help='YAML file containing score adjustments (optional)',
    )
    score_parser.add_argument(
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
    pyzy activity a1.csv a2.csv -l gradebooks/*.csv -n "W1 CA" "W2 PA"
    pyzy activity a*.csv -l gradebooks/*.csv -n "CA"  (aggregate: best score + submission count)
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
        help='Column name(s): one per input file, or one name to aggregate all into a single column',
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

    if args.command == 'score':
        from .score import run_score
        run_score(
            deadline_dir=args.deadline,
            lifted_dir=args.lifted,
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
        )


if __name__ == '__main__':
    main()
