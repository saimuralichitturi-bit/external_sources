"""
pipeline/categorize.py
======================
Auto-categorizes products in unified_snapshots.csv into a normalized
L1 taxonomy using category_rules.json.

Rules file (data/category_rules.json) has two match methods per category:
  1. platform_categories — exact match against the raw `category` column
  2. keywords           — substring match against product name (case-insensitive)

Priority: platform_categories > keywords > "Uncategorized"

Adds / overwrites two columns:
  category_raw  — original platform category (preserved)
  category      — normalized L1 category

Usage:
  python pipeline/categorize.py
  python pipeline/categorize.py --data-dir data --rules data/category_rules.json
"""

import os, sys, json, argparse
from pathlib import Path

import pandas as pd

ROOT       = Path(__file__).parent.parent
DATA_DIR   = ROOT / "data"
RULES_FILE = DATA_DIR / "category_rules.json"


def load_rules(rules_path: str) -> dict:
    with open(rules_path, encoding="utf-8") as f:
        return json.load(f)


def build_platform_cat_map(rules: dict) -> dict:
    """Return {platform_category_lower: L1_category}"""
    mapping = {}
    for l1, cfg in rules.items():
        for pc in cfg.get("platform_categories", []):
            mapping[pc.lower().strip()] = l1
    return mapping


def build_keyword_list(rules: dict) -> list[tuple[str, str]]:
    """Return [(keyword_lower, L1_category), ...] sorted longest-first."""
    pairs = []
    for l1, cfg in rules.items():
        for kw in cfg.get("keywords", []):
            pairs.append((kw.lower().strip(), l1))
    # longest keywords first → more specific matches win
    pairs.sort(key=lambda x: -len(x[0]))
    return pairs


def assign_category(name: str, raw_cat: str,
                    pc_map: dict, kw_list: list) -> str:
    # 1. Exact platform_category match
    rc = str(raw_cat).strip().lower()
    if rc in pc_map:
        return pc_map[rc]

    # 2. Keyword match against product name
    name_lower = str(name).lower()
    for kw, l1 in kw_list:
        if kw in name_lower:
            return l1

    # 3. Keyword match against raw category (for partial matches)
    for kw, l1 in kw_list:
        if kw in rc:
            return l1

    return "Uncategorized"


def categorize(data_dir: str, rules_path: str) -> pd.DataFrame:
    snap_path = os.path.join(data_dir, "unified_snapshots.csv")
    if not os.path.exists(snap_path):
        print(f"ERROR: {snap_path} not found. Run consolidate.py first.")
        sys.exit(1)

    rules   = load_rules(rules_path)
    pc_map  = build_platform_cat_map(rules)
    kw_list = build_keyword_list(rules)

    df = pd.read_csv(snap_path, dtype=str)

    # Preserve original category
    if "category_raw" not in df.columns:
        df.insert(df.columns.get_loc("category") + 1,
                  "category_raw", df["category"])

    df["category"] = df.apply(
        lambda r: assign_category(
            r.get("name", ""),
            r.get("category_raw") or r.get("category", ""),
            pc_map, kw_list
        ),
        axis=1,
    )

    # Stats
    counts = df["category"].value_counts()
    uncategorized = counts.get("Uncategorized", 0)
    total = len(df)
    print(f"\nCategorization complete: {total:,} rows")
    print(f"  Coverage : {total - uncategorized:,} / {total:,} "
          f"({(total - uncategorized) / total * 100:.1f}%)")
    print(f"  Uncategorized: {uncategorized:,}")
    print(f"\nL1 category distribution:")
    for cat, cnt in counts.items():
        bar = "#" * int(cnt / total * 40)
        print(f"  {cat:<30} {cnt:>5}  {bar}")

    df.to_csv(snap_path, index=False)
    print(f"\n[done] {snap_path}")
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default=str(DATA_DIR))
    parser.add_argument("--rules",    default=str(RULES_FILE))
    args = parser.parse_args()
    categorize(args.data_dir, args.rules)
