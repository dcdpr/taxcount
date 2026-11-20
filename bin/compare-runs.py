#!/usr/bin/env python3
"""Compare the output files of two taxcount runs.

Usage:
  compare-runs.py PREFIX_A PREFIX_B
  compare-runs.py --mrt

Examples:
  bin/compare-runs.py fixtures/runs/kraken-tests/20261115- runs/kraken-tests/20261120-
  bin/compare-runs.py --mrt

The script compares each file PREFIX_A<name> with the file PREFIX_B<name>.
For each pair of different files, the script shows a diff.

With the `--mrt` option (most-recent-test), the script finds the two
prefixes.  The directories are `fixtures/runs/kraken-tests/` and
`runs/kraken-tests/`.  In each directory, the script finds the last date at
the start of a file name.  The prefix is the directory, then that date, then
a hyphen.  The script finds the two directories from the path of the script
file.

In a checkpoint file, the script ignores the `time` value of the checkpoint
header.  That value is the time of the run, and it is different in each run.

Exit status:
  0  the files of each pair are equal
  1  the files of a pair are different, or a file is without a pair
  2  the script found an error
"""

import argparse
import difflib
import glob
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FIXTURE_RUNS = ROOT / "fixtures" / "runs" / "kraken-tests"
TEST_RUNS = ROOT / "runs" / "kraken-tests"
RUN_DATE = re.compile(r"(\d{8})-")


class NoFiles(Exception):
    """This error is for a prefix that is without files."""

    def __str__(self):
        return f"no file starts with {self.args[0]}"


class NoRun(Exception):
    """This error is for a directory without the files of a run."""

    def __str__(self):
        return f"a file with a YYYYMMDD- prefix is missing from {self.args[0]}"


def last_prefix(directory):
    """Return the prefix `directory/YYYYMMDD-` of the last run in `directory`.

    The name of each file of a run starts with the date of that run.
    """
    directory = os.path.relpath(directory)
    dates = {
        match.group(1)
        for name in os.listdir(directory)
        if (match := RUN_DATE.match(name))
    }
    if not dates:
        raise NoRun(directory)
    return os.path.join(directory, max(dates) + "-")


def names(prefix):
    """Return the name of each file that starts with `prefix`, without `prefix`."""
    paths = glob.glob(glob.escape(prefix) + "*")
    found = {path[len(prefix):] for path in paths if os.path.isfile(path)}
    if not found:
        raise NoFiles(prefix)
    return found


def mute_header_time(lines):
    """Replace the `time` value of the checkpoint header in `lines`.

    The time of the run is there, from CheckpointHeader::default().  The
    script compares each other `time` line of the checkpoint.
    """
    in_header = False
    for index, line in enumerate(lines):
        if line.strip() == "header: (":
            in_header = True
        elif in_header and line.lstrip().startswith("time: "):
            indent = line[: len(line) - len(line.lstrip())]
            lines[index] = indent + 'time: "<time of the run>",'
            return


def read_lines(path):
    """Return the lines of the file at `path`, without the time of the run."""
    lines = path.read_text().splitlines()
    if path.suffix == ".ron":
        mute_header_time(lines)
    return lines


def compare(prefix_a, prefix_b):
    """Show the differences between the two sets of files.

    If the script finds a difference, return True.
    """
    names_a, names_b = names(prefix_a), names(prefix_b)
    different = False
    for name in sorted(names_a | names_b):
        path_a, path_b = Path(prefix_a + name), Path(prefix_b + name)
        if name not in names_a or name not in names_b:
            missing = path_a if name not in names_a else path_b
            print(f"missing: {missing}")
            different = True
            continue
        diff = list(
            difflib.unified_diff(
                read_lines(path_a),
                read_lines(path_b),
                str(path_a),
                str(path_b),
                lineterm="",
            )
        )
        if diff:
            print("\n".join(diff))
            different = True
        else:
            print(f"same: {name}")
    return different


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "prefix_a",
        metavar="PREFIX_A",
        nargs="?",
        help="the start of the path of each file of the first run",
    )
    parser.add_argument(
        "prefix_b",
        metavar="PREFIX_B",
        nargs="?",
        help="the start of the path of each file of the second run",
    )
    parser.add_argument(
        "--mrt",
        action="store_true",
        help="most-recent-test: compare the last run in fixtures/runs/kraken-tests/"
        " with the last run in runs/kraken-tests/",
    )
    args = parser.parse_args()
    prefixes = [
        prefix for prefix in (args.prefix_a, args.prefix_b) if prefix is not None
    ]
    if args.mrt and prefixes:
        parser.error("the --mrt option replaces PREFIX_A and PREFIX_B")
    if not args.mrt and len(prefixes) != 2:
        parser.error("give PREFIX_A and PREFIX_B, or give --mrt")
    try:
        if args.mrt:
            prefix_a, prefix_b = last_prefix(FIXTURE_RUNS), last_prefix(TEST_RUNS)
        else:
            # A quoted shell pattern can end with "*".
            prefix_a, prefix_b = (prefix.rstrip("*") for prefix in prefixes)
        different = compare(prefix_a, prefix_b)
    except (NoFiles, NoRun, OSError) as err:
        print(f"error: {err}", file=sys.stderr)
        return 2
    return 1 if different else 0


if __name__ == "__main__":
    sys.exit(main())
