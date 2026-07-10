#!/usr/bin/env python3
"""Pair each content release with the program manifest live at its patch.

Reads <game>-content.tsv and <game>-program.tsv (both epoch<TAB>date<TAB>manifest),
writes <game>-pairing.tsv (content_epoch<TAB>content_manifest<TAB>program_manifest).

A content patch and its program (client) update are usually pushed at the same
second, so we take the latest program manifest with epoch <= content_epoch + a
small grace window — enough to catch a client build landing moments after its
content push, without jumping to the next patch.

Usage: pair-programs.py <game>   (game = poe1 | poe2)
"""
import bisect
import sys
from pathlib import Path

GRACE = 900  # seconds

def load(path):
    rows = []
    for ln in Path(path).read_text().splitlines()[1:]:
        e, d, m = ln.split("\t")
        rows.append((int(e), d, m))
    return sorted(rows)

def main():
    game = sys.argv[1]
    here = Path(__file__).parent
    content = load(here / f"{game}-content.tsv")
    program = load(here / f"{game}-program.tsv")
    pe = [e for e, _, _ in program]

    lines = ["content_epoch\tcontent_manifest\tprogram_manifest"]
    unpaired = 0
    for ce, _cd, cm in content:
        i = bisect.bisect_right(pe, ce + GRACE) - 1
        if i < 0:
            unpaired += 1
            continue
        lines.append(f"{ce}\t{cm}\t{program[i][2]}")
    (here / f"{game}-pairing.tsv").write_text("\n".join(lines) + "\n")
    print(f"{game}: {len(content)} content, {len(program)} program, "
          f"{len(lines)-1} paired, {unpaired} unpaired")

if __name__ == "__main__":
    main()
