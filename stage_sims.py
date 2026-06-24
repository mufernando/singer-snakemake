#!/usr/bin/env python3
"""
Stage per-chromosome inputs from many simulation subfolders into a single flat
directory, so the SINGER workflow can run over all sims in one Snakemake DAG.

Each simulation subfolder under SIMS_PARENT_DIR holds per-chromosome inputs named
"<chrom>.<suffix>" (e.g. 1.vcf.gz, 1.hapmap, 1.mask.bed, ...). Snakemake's {chrom}
wildcard can't cross "/", so this creates symlinks in STAGING_DIR named
"<sim>__<chrom>.<suffix>", encoding sim + chrom into a single wildcard token.

Afterwards, point `input-dir` at STAGING_DIR (and leave `chromosomes: null`) in
your config. Outputs will land under `output-dir/<sim>__<chrom>/`.

Re-run any time (e.g. after adding sims); it is idempotent and only touches links.

Usage:
    python stage_sims.py SIMS_PARENT_DIR STAGING_DIR [--clean] [--dry-run]
"""
import argparse
import os
import re
import sys

# Per-chromosome input suffixes the workflow looks for, derived from the VCF path
# in workflow/scripts/chunk_chromosomes.py (.vcf.gz -> .hapmap/.mask.bed/etc).
INPUT_SUFFIXES = (
    ".vcf.gz",
    ".vcf.gz.tbi",
    ".hapmap",
    ".mask.bed",
    ".meta.csv",
    ".ancestral.fa.gz",
    ".filter.txt",
    ".omit.txt",
)
# Per-chrom inputs start with an integer chromosome label (e.g. "1.vcf.gz"); this
# excludes sim-level files like "_dtwf.trees", "chrom_names.pkl", "demography.pkl".
CHROM_FILE_RE = re.compile(r"^[0-9]+\.")


def iter_input_files(sim_dir):
    for fname in sorted(os.listdir(sim_dir)):
        if CHROM_FILE_RE.match(fname) and fname.endswith(INPUT_SUFFIXES):
            yield fname


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "sims_parent_dir",
        help="directory containing one subfolder per simulation",
    )
    parser.add_argument(
        "staging_dir",
        help="flat directory to populate with symlinks (created if absent)",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="remove existing symlinks in staging_dir first (never touches real files)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print what would be linked without creating anything",
    )
    args = parser.parse_args()

    parent = os.path.abspath(args.sims_parent_dir)
    staging = os.path.abspath(args.staging_dir)
    if not os.path.isdir(parent):
        sys.exit(f"error: sims_parent_dir not found: {parent}")

    if not args.dry_run:
        os.makedirs(staging, exist_ok=True)

    if args.clean and os.path.isdir(staging):
        for fname in os.listdir(staging):
            path = os.path.join(staging, fname)
            if os.path.islink(path):
                print(f"rm {path}") if args.dry_run else os.remove(path)

    n_sims = n_chroms = n_links = 0
    for sim in sorted(os.listdir(parent)):
        sim_dir = os.path.join(parent, sim)
        if not os.path.isdir(sim_dir):
            continue
        sim_links = 0
        for fname in iter_input_files(sim_dir):
            target = os.path.join(sim_dir, fname)
            link = os.path.join(staging, f"{sim}__{fname}")
            if fname.endswith(".vcf.gz"):
                n_chroms += 1
            if args.dry_run:
                print(f"{link} -> {target}")
                sim_links += 1
                continue
            if os.path.islink(link):
                if os.readlink(link) == target:
                    continue  # already correct
                os.remove(link)
            elif os.path.exists(link):
                sys.exit(f"error: refusing to overwrite non-symlink: {link}")
            os.symlink(target, link)
            sim_links += 1
        if sim_links:
            n_sims += 1
            n_links += sim_links

    verb = "would stage" if args.dry_run else "staged"
    print(
        f"{verb} {n_links} symlinks ({n_chroms} chromosomes across {n_sims} sims) "
        f"into {staging}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
