#!/usr/bin/env python3
"""Emit diagnostic identities for review without publishing compiler logs.

This is evidence, never an acceptance baseline. The gate still compares against
the separately captured immutable old-source inventory.
"""
import argparse
from common import git, syntax, tracked_sources, write_json
from diagnostics import parse, records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--clippy', required=True)
    parser.add_argument('--out', required=True)
    args = parser.parse_args()
    sources = tracked_sources()
    metrics = {}
    warnings, failures = parse(args.clippy, sources, syntax(sources), metrics=metrics)
    write_json(args.out, {'commit': git('rev-parse', 'HEAD'),
                         'warnings': records(warnings, metrics), 'failures': failures})
    return bool(failures)


if __name__ == '__main__':
    raise SystemExit(main())
