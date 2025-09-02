#!/usr/bin/env python3
"""
Merge IEEE KBART subjects into a usage file and build a weighted subject ranking.

Features:
- Auto-detects usage sheet & header row for Excel; also supports CSV/Parquet usage files.
- Vectorized merge: Online ISSN (normalized) first, then Title (casefolded/NFC).
- Subject aliases mapping from CSV (optional).
- Per-group rankings (optional).
- Safe, capped fuzzy Title fallback using rapidfuzz (optional).
- Polished Excel outputs + CSV/MD/JSON artifacts + rotating log file.

Outputs:
    • <prefix>_sample_with_subjects.xlsx  (Data, Subject Ranking)
    • <prefix>_full_with_subjects.xlsx    (Data, Subject Ranking)
    • subject_ranking.csv
    • subject_ranking_by_group.csv   (if --group-by)
    • report.md
    • run_info.json
    • data_issues.csv (if any)
    • .kbart_title_tbl.parquet / .kbart_issn_tbl.parquet (cache, if pyarrow installed)

Usage:
    python merge_ieee_subjects.py --usage usage.xlsx --kbart IEEEXplore_Global_IEL.xlsx

Recommended requirements:
    pandas>=2.1, openpyxl>=3.1
Optional:
    pyarrow>=14.0 (parquet cache), rapidfuzz>=3.6 (fuzzy title fallback)
"""

import sys
import re
import logging
import unicodedata
from pathlib import Path
from typing import Optional, Iterable, List
from datetime import datetime
import json

import pandas as pd

# ------------------------ CLI & Logging ------------------------
def parse_args():
    import argparse
    p = argparse.ArgumentParser(description="Merge IEEE KBART subjects into usage.xlsx and rank subjects.")
    p.add_argument("--usage", default="usage.xlsx", help="Path to usage file (Excel/CSV/Parquet)")
    p.add_argument("--kbart", default="IEEEXplore_Global_IEL.xlsx", help="Path to IEEE KBART file (Excel)")
    p.add_argument("--kbart-sheet", default="Kbart Data", help="KBART sheet name (default: 'Kbart Data')")
    p.add_argument("--sample-rows", type=int, default=20, help="Rows for demo sample (default: 20)")
    p.add_argument("--out-prefix", default="usage", help="Filename prefix for Excel outputs (default: 'usage')")
    p.add_argument("--alias-file", default=None, help="CSV with columns: raw,canonical (case-insensitive mapping)")
    p.add_argument("--group-by", default=None, help="Comma-separated column names to compute per-group rankings")
    p.add_argument("--top-n", type=int, default=20, help="Top N subjects to show in report.md (default: 20)")
    # Fuzzy matching flags
    p.add_argument("--enable-fuzzy", action="store_true", help="Enable fuzzy Title fallback for unmatched rows")
    p.add_argument("--fuzzy-threshold", type=int, default=96, help="Fuzzy score cutoff (default: 96)")
    p.add_argument("--fuzzy-max", type=int, default=2000, help="Max unmatched titles to attempt fuzzing (default: 2000)")
    p.add_argument("--dry-run", action="store_true", help="Do not write Excel outputs")
    p.add_argument("--verbose", "-v", action="count", default=0, help="Increase log verbosity (-v/-vv)")
    return p.parse_args()


def setup_logging(verbosity: int):
    level = logging.WARNING if verbosity == 0 else (logging.INFO if verbosity == 1 else logging.DEBUG)
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")


log = logging.getLogger(__name__)

# --- File logging (rotating run.log) ---
from logging.handlers import RotatingFileHandler

def add_file_logging(log_path: str = "run.log"):
    """Attach a rotating file handler to root logger."""
    fh = RotatingFileHandler(log_path, maxBytes=2_000_000, backupCount=3, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
    root = logging.getLogger()
    root.addHandler(fh)
    log.info(f"File logging enabled: {log_path}")

# ------------------------ Utilities ------------------------
def norm_col(s: str) -> str:
    """Normalize a column/header string for matching."""
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", "_", s)
    s = s.replace("-", "_")
    s = re.sub(r"[^\w\.\_]", "", s)  # keep dot for subjects.1
    return s


def find_column(df: pd.DataFrame, candidates, must_contain_all=None) -> Optional[str]:
    """Find a column matching any candidate or fuzzy tokens."""
    if isinstance(candidates, str):
        candidates = [candidates]
    norm_map = {col: norm_col(str(col)) for col in df.columns}
    reverse = {v: k for k, v in norm_map.items()}
    # Exact candidate matches
    for cand in candidates:
        c = norm_col(cand)
        if c in reverse:
            return reverse[c]
    # Fuzzy contains
    if must_contain_all:
        toks = [norm_col(t) for t in must_contain_all]
        for col, n in norm_map.items():
            if all(t in n for t in toks):
                return col
    return None


def find_subject_columns(df: pd.DataFrame):
    """Detect all columns that look like subject tags."""
    cols = []
    for col in df.columns:
        n = norm_col(str(col))
        if n == "subject" or n.startswith("subjects") or "subject" in n:
            cols.append(col)
    # prefer subjects, subjects.1, ... first
    dot_first = [c for c in cols if norm_col(str(c)).startswith("subjects")]
    others = [c for c in cols if c not in dot_first]
    return dot_first + others


def normalize_strings(s):
    if pd.isna(s):
        return ""
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    s = unicodedata.normalize("NFC", s)
    return s


def normalize_key(s):
    return normalize_strings(s).casefold()


def normalize_issn(s):
    s = normalize_strings(s)
    s = re.sub(r"[^0-9Xx]", "", s)  # keep digits and X
    return s.upper()

# ------------------------ Usage loading ------------------------
def sniff_usage_sheet_and_header(usage_path: Path):
    """
    Scan first ~40 rows for a header line containing 'title' and either 'reporting_period_total' or months.
    """
    xl = pd.ExcelFile(usage_path)
    best = None
    best_score = -1

    month_tokens = {"jan", "feb", "mar", "apr", "may", "jun",
                    "jul", "aug", "sep", "oct", "nov", "dec"}

    for sheet in xl.sheet_names:
        try:
            preview = pd.read_excel(usage_path, sheet_name=sheet, header=None, nrows=60)
        except Exception:
            continue

        rows = min(40, len(preview))
        for r in range(rows):
            vals = preview.iloc[r].tolist()
            norm = [norm_col(str(v)) for v in vals if pd.notna(v)]
            if not norm:
                continue

            has_title = any("title" == n or "title" in n for n in norm)
            has_rpt = any("reporting_period_total" in n for n in norm)
            has_month = any(n.split("_")[0] in month_tokens for n in norm)

            if not has_title:
                continue

            score = 0
            score += 2 if has_rpt else 0
            score += 1 if has_month else 0
            for token in ("publisher", "platform", "doi", "online_issn", "print_issn", "uri"):
                if any(token in n for n in norm):
                    score += 0.2

            if score > best_score:
                best = (sheet, r)
                best_score = score

    if best is None:
        log.warning("Could not confidently detect header; falling back to first sheet & row 0")
        return xl.sheet_names[0], 0

    log.info(f"Detected usage sheet '{best[0]}' with header row {best[1]} (score {best_score})")
    return best


def load_usage_autodetect_excel(usage_path: Path) -> pd.DataFrame:
    sheet, header_row = sniff_usage_sheet_and_header(usage_path)
    df = pd.read_excel(usage_path, sheet_name=sheet, header=header_row)

    # Drop a repeated header row if present
    try:
        header_norm = [norm_col(str(c)) for c in df.columns.tolist()]
        first_norm = [norm_col(str(x)) for x in df.iloc[0].tolist()]
        matches = sum(1 for a, b in zip(first_norm, header_norm) if a == b)
        if matches >= max(3, int(0.5 * len(header_norm))):
            df = df.drop(df.index[0]).reset_index(drop=True)
            log.debug("Dropped repeated header row from data")
    except Exception:
        pass

    return df


def load_usage_any(usage_path: Path) -> pd.DataFrame:
    """Load usage from Excel/CSV/Parquet; Excel gets auto-detection for sheet/header."""
    suffix = usage_path.suffix.lower()
    if suffix in {".xlsx", ".xlsm", ".xls"}:
        return load_usage_autodetect_excel(usage_path)
    if suffix == ".csv":
        return pd.read_csv(usage_path)
    if suffix == ".parquet":
        return pd.read_parquet(usage_path)
    # default to Excel attempt
    return load_usage_autodetect_excel(usage_path)

# ------------------------ KBART processing ------------------------
def build_kbart_lookups(kbart_df: pd.DataFrame):
    title_col = find_column(kbart_df, ["publication_title", "publication title", "title"], must_contain_all=["title"])
    issn_col = find_column(
        kbart_df,
        ["Online_ISSN", "online_issn", "online identifier", "online_identifier", "eissn", "e-issn", "e_issn"],
        must_contain_all=["online", "issn"],
    )
    if not title_col:
        raise ValueError("Could not find a publication title column in KBART.")
    if not issn_col:
        log.warning("Online ISSN column not found in KBART; will match by Title only.")

    subject_cols = find_subject_columns(kbart_df)
    if not subject_cols:
        raise ValueError("Could not find any subject columns in KBART (names containing 'subject').")

    log.info(f"KBART columns -> Title: '{title_col}' | ISSN: '{issn_col or 'N/A'}' | Subjects: {len(subject_cols)} cols")
    return issn_col, title_col, subject_cols


# --- Cached KBART subject tables (Parquet) ---
def build_kbart_subject_table(kbart_df: pd.DataFrame, subject_cols: list[str], title_col: str, issn_col: Optional[str]):
    """Return tables for ISSN and Title joins with normalized keys. Uses a Parquet cache when available."""
    cache_title = Path(".kbart_title_tbl.parquet")
    cache_issn = Path(".kbart_issn_tbl.parquet") if issn_col else None

    def build_title():
        tbl = kbart_df[[title_col] + subject_cols].copy()
        tbl[title_col] = tbl[title_col].map(normalize_key)
        tbl = (
            tbl.dropna(subset=[title_col])
               .loc[tbl[title_col] != ""]
               .drop_duplicates(subset=[title_col])
               .rename(columns={title_col: "__norm_title"})
        )
        return tbl

    def build_issn():
        if not issn_col:
            return None
        tbl = kbart_df[[issn_col] + subject_cols].copy()
        tbl[issn_col] = tbl[issn_col].map(normalize_issn)
        tbl = (
            tbl.dropna(subset=[issn_col])
               .loc[tbl[issn_col] != ""]
               .drop_duplicates(subset=[issn_col])
               .rename(columns={issn_col: "__norm_issn"})
        )
        return tbl

    # Load or build title cache
    if cache_title.exists():
        try:
            kbart_title_tbl = pd.read_parquet(cache_title)
            log.info(f"Loaded KBART title table from cache: {cache_title}")
        except Exception as e:
            log.warning(f"Failed to read title cache ({e}); rebuilding cache.")
            kbart_title_tbl = build_title()
            _safe_to_parquet(kbart_title_tbl, cache_title)
    else:
        kbart_title_tbl = build_title()
        _safe_to_parquet(kbart_title_tbl, cache_title)

    # Load or build issn cache
    if issn_col:
        if cache_issn.exists():
            try:
                kbart_issn_tbl = pd.read_parquet(cache_issn)
                log.info(f"Loaded KBART ISSN table from cache: {cache_issn}")
            except Exception as e:
                log.warning(f"Failed to read ISSN cache ({e}); rebuilding cache.")
                kbart_issn_tbl = build_issn()
                _safe_to_parquet(kbart_issn_tbl, cache_issn)
        else:
            kbart_issn_tbl = build_issn()
            _safe_to_parquet(kbart_issn_tbl, cache_issn)
    else:
        kbart_issn_tbl = None

    return kbart_title_tbl, kbart_issn_tbl


def _safe_to_parquet(df: pd.DataFrame, path: Path):
    """Write parquet if possible; log a hint if pyarrow is missing."""
    try:
        df.to_parquet(path, index=False)
    except Exception as e:
        log.warning(f"Parquet cache not written ({e}). Install pyarrow for faster repeated runs.")

# ------------------------ Merge + Ranking ------------------------
def attach_subjects_vectorized(
    usage_df: pd.DataFrame,
    subject_cols: list[str],
    usage_title_col: str,
    usage_issn_col: Optional[str],
    kbart_title_tbl: pd.DataFrame,
    kbart_issn_tbl: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """Attach subjects; prefer ISSN matches, then fill remaining via Title. Adds Matched_By and _issues."""
    df = usage_df.copy()

    # Ensure subject columns exist and create marker column
    for c in subject_cols:
        if c not in df.columns:
            df[c] = pd.NA
    df["Matched_By"] = pd.NA  # track which key filled the subjects

    # Normalize keys
    df["__norm_title"] = df[usage_title_col].map(normalize_key)
    if usage_issn_col:
        df["__norm_issn"] = df[usage_issn_col].map(normalize_issn)

    # Merge by ISSN first (preferred)
    if kbart_issn_tbl is not None and usage_issn_col:
        pre_fill_mask = df[subject_cols].isna().all(axis=1)
        df = df.merge(kbart_issn_tbl, on="__norm_issn", how="left", suffixes=("", "__kbart_issn"))
        any_issn_hit = pd.Series(False, index=df.index)
        for c in subject_cols:
            kb_col = f"{c}__kbart_issn"
            if kb_col in df.columns:
                # rows that got subjects via ISSN
                fill_mask = df[c].isna() & df[kb_col].notna()
                any_issn_hit |= fill_mask
                df.loc[fill_mask, c] = df.loc[fill_mask, kb_col]
        df.loc[any_issn_hit & pre_fill_mask, "Matched_By"] = "ISSN"
        # Cleanup kb suffix cols
        drop_cols = [f"{c}__kbart_issn" for c in subject_cols]
        df.drop(columns=[c for c in drop_cols if c in df.columns], inplace=True, errors="ignore")

    # For rows still missing subjects, merge by title
    missing = df[subject_cols].isna().all(axis=1)
    if missing.any():
        pre_fill_mask = missing.copy()
        fill = df.loc[missing, ["__norm_title"]].merge(kbart_title_tbl, on="__norm_title", how="left")
        got_title = pd.Series(False, index=df.index)
        for c in subject_cols:
            new_vals = fill[c]
            fill_mask = df.loc[missing, c].isna() & new_vals.notna()
            idxs = df.loc[missing].index[fill_mask]
            df.loc[idxs, c] = new_vals[fill_mask].values
            got_title.loc[idxs] = True
        df.loc[got_title & pre_fill_mask & df["Matched_By"].isna(), "Matched_By"] = "Title"

    # Cleanup helpers
    df.drop(columns=["__norm_title", "__norm_issn"], errors="ignore", inplace=True)

    # --- Build a data_issues frame (optional but useful) ---
    issues = []
    # Flag: rows that had both an ISSN and a Title but matched only by Title
    if usage_issn_col:
        has_both_keys = df[usage_issn_col].notna() & (df[usage_issn_col].astype(str).str.strip() != "") & \
                        df[usage_title_col].notna() & (df[usage_title_col].astype(str).str.strip() != "")
        only_title = (df["Matched_By"] == "Title") & has_both_keys
        if only_title.any():
            cols_to_keep = [usage_title_col, usage_issn_col, "Matched_By"] + subject_cols
            issues.append(df.loc[only_title, cols_to_keep].assign(_issue="ISSN/title mismatch (title used)"))

    # Flag: completely unmatched rows (no subject found)
    unmatched = df[subject_cols].isna().all(axis=1)
    if unmatched.any():
        cols_to_keep = [usage_title_col] + ([usage_issn_col] if usage_issn_col else []) + subject_cols + ["Matched_By"]
        issues.append(df.loc[unmatched, cols_to_keep].assign(_issue="No subject matched"))

    df._issues = pd.concat(issues, ignore_index=True) if issues else pd.DataFrame()

    return df


def compute_subject_ranking(df: pd.DataFrame, subject_cols: list[str], rpt_col: str) -> pd.DataFrame:
    """Weighted ranking: each subject gets +Reporting_Period_Total for every row where it appears."""
    # Clean RPT
    rpt = pd.to_numeric(df[rpt_col].astype(str).str.replace(",", "", regex=False), errors="coerce").fillna(0.0)

    # Wide -> long
    long = (
        df.assign(__rpt=rpt)[subject_cols + ["__rpt"]]
        .melt(id_vars="__rpt", value_name="subject")
        .dropna(subset=["subject"])
    )
    long["subject"] = long["subject"].astype(str).str.strip()
    long = long[long["subject"] != ""]

    agg = (
        long.groupby("subject", as_index=False)
        .agg(occurrence=("__rpt", "sum"), rows_with_subject=("__rpt", "size"))
        .sort_values(["occurrence", "rows_with_subject"], ascending=[False, False])
        .reset_index(drop=True)
    )
    return agg


def compute_subject_ranking_by_group(df: pd.DataFrame, group_cols: List[str], subject_cols: list[str], rpt_col: str) -> pd.DataFrame:
    """Per-group subject ranking; returns a long frame with group columns + subject + occurrence + rows_with_subject."""
    # Prepare weighted rows
    rpt = pd.to_numeric(df[rpt_col].astype(str).str.replace(",", "", regex=False), errors="coerce").fillna(0.0)
    base = df[group_cols].copy()
    base["__rpt"] = rpt

    # stack subject columns
    long = (
        pd.concat([base, df[subject_cols]], axis=1)
        .melt(id_vars=group_cols + ["__rpt"], value_name="subject")
        .dropna(subset=["subject"])
    )
    long["subject"] = long["subject"].astype(str).str.strip()
    long = long[long["subject"] != ""]

    agg = (
        long.groupby(group_cols + ["subject"], as_index=False)
            .agg(occurrence=("__rpt", "sum"),
                 rows_with_subject=("__rpt", "size"))
            .sort_values(group_cols + ["occurrence","rows_with_subject"], ascending=[True]*len(group_cols) + [False, False])
            .reset_index(drop=True)
    )
    return agg
def summarize_matches(df: pd.DataFrame, subject_cols: list[str], tag: str = "full"):
    """Log how many rows have at least one subject attached."""
    if not subject_cols:
        log.info(f"[{tag}] rows: {len(df):,} | no subject columns detected")
        return

    rows = len(df)
    if rows == 0:
        log.info(f"[{tag}] rows: 0 | with ≥1 subject: 0 (0.0%)")
        return

    has_subject = ~(df[subject_cols].isna().all(axis=1))
    matched = int(has_subject.sum())
    pct = float(has_subject.mean() * 100.0)
    log.info(f"[{tag}] rows: {rows:,} | with ≥1 subject: {matched:,} ({pct:.1f}%)")
# ------------------------ Subject aliasing ------------------------
def load_subject_aliases(alias_file: Optional[str]):
    """Load subject aliases CSV with columns raw,canonical. Returns a mapping (lowercased raw -> canonical)."""
    if not alias_file:
        return {}
    path = Path(alias_file)
    if not path.exists():
        log.warning(f"Alias file not found: {alias_file}")
        return {}
    df = pd.read_csv(path)
    if not {"raw","canonical"}.issubset(set(df.columns.str.lower())):
        log.warning("Alias file must contain columns: raw, canonical (case-insensitive). Ignoring.")
        return {}
    col_map = {c.lower(): c for c in df.columns}
    raw_col = col_map.get("raw")
    can_col = col_map.get("canonical")
    m = {}
    for raw, can in zip(df[raw_col], df[can_col]):
        raw_key = normalize_strings(raw).casefold()
        if raw_key:
            m[raw_key] = normalize_strings(can)
    log.info(f"Loaded {len(m):,} subject aliases")
    return m


def apply_subject_aliases_inplace(df: pd.DataFrame, subject_cols: list[str], alias_map: dict):
    """Map subject values in the data frame using alias_map (case-insensitive raw -> canonical)."""
    if not alias_map:
        return
    for c in subject_cols:
        s = df[c].astype(str).fillna("").map(lambda x: alias_map.get(normalize_strings(x).casefold(), x))
        df[c] = s.where(s != "", pd.NA)

# ------------------------ Fuzzy fallback ------------------------
def fuzzy_title_fill(df: pd.DataFrame,
                     kbart_title_tbl: pd.DataFrame,
                     usage_title_col: str,
                     subject_cols: list[str],
                     threshold: int,
                     max_unmatched: int):
    """
    Attempt fuzzy matching for rows with no subjects after ISSN/Title exact merges.
    Uses rapidfuzz token_sort_ratio with high threshold and a cap on attempts.
    """
    try:
        from rapidfuzz import fuzz, process
    except Exception as e:
        log.warning(f"Fuzzy matching requested but rapidfuzz not available ({e}). Skipping fuzzy fallback.")
        return df  # no change

    # Identify unmatched rows
    unmatched_mask = df[subject_cols].isna().all(axis=1)
    if not unmatched_mask.any():
        return df

    # Prepare candidates
    usage_titles = df.loc[unmatched_mask, usage_title_col].astype(str).str.strip()
    usage_norm = usage_titles.map(lambda x: normalize_key(x))
    usage_unique = usage_norm.unique()
    if len(usage_unique) > max_unmatched:
        log.warning(f"Unmatched titles ({len(usage_unique)}) exceed --fuzzy-max ({max_unmatched}); truncating.")
        usage_unique = usage_unique[:max_unmatched]

    choices = kbart_title_tbl["__norm_title"].astype(str).tolist()
    title_to_subjects = kbart_title_tbl.set_index("__norm_title")[subject_cols]

    # Build mapping via rapidfuzz
    mapping = {}
    for ut in usage_unique:
        if not ut:
            continue
        match = process.extractOne(ut, choices, scorer=fuzz.token_sort_ratio, score_cutoff=threshold)
        if match:
            mapping[ut] = match[0]

    if not mapping:
        log.info("Fuzzy fallback: no matches above threshold.")
        return df

    # Construct a frame to join back
    mm = pd.DataFrame({"__norm_title": list(mapping.keys()),
                       "__matched_title": list(mapping.values())})
    mm = mm.merge(title_to_subjects, left_on="__matched_title", right_index=True, how="left").drop(columns="__matched_title")

    # Apply fills where still missing
    still_missing = df[subject_cols].isna().all(axis=1)
    fill = df.loc[still_missing, ["__norm_title"]].merge(mm, on="__norm_title", how="left")
    got_fuzzy = pd.Series(False, index=df.index)
    for c in subject_cols:
        new_vals = fill[c]
        mask = df.loc[still_missing, c].isna() & new_vals.notna()
        idxs = df.loc[still_missing].index[mask]
        df.loc[idxs, c] = new_vals[mask].values
        got_fuzzy.loc[idxs] = True

    df.loc[got_fuzzy & df.get("Matched_By").isna(), "Matched_By"] = "Title(Fuzzy)"
    return df

# ------------------------ Reporting helpers ------------------------
def write_subject_artifacts(full_rank: pd.DataFrame, top_n: int = 20):
    """Write subject_ranking.csv and a simple markdown report with Top N."""
    full_rank.to_csv("subject_ranking.csv", index=False)

    lines = []
    lines.append(f"# Subject Ranking Report")
    lines.append(f"_Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}_")
    lines.append("")
    lines.append("## Top subjects")
    lines.append("")
    head = full_rank.head(top_n).copy()
    lines.append(head.to_string(index=False))
    lines.append("")
    Path("report.md").write_text("\n".join(lines), encoding="utf-8")
    log.info(f"Wrote subject_ranking.csv and report.md")


def write_subject_artifacts_by_group(by_group_rank: pd.DataFrame, group_cols: List[str], top_n: int):
    """Write per-group ranking CSV and append to report.md."""
    if by_group_rank is None or by_group_rank.empty:
        return
    by_group_rank.to_csv("subject_ranking_by_group.csv", index=False)
    # Append to report.md
    report_path = Path("report.md")
    if report_path.exists():
        text = report_path.read_text(encoding="utf-8")
    else:
        text = "# Subject Ranking Report\n\n"
    lines = [text, "## Top subjects by group", ""]
    # For each group, show top N
    for keys, g in by_group_rank.groupby(group_cols):
        title = " | ".join(f"{col}={val}" for col, val in zip(group_cols, keys if isinstance(keys, Iterable) and not isinstance(keys, str) else [keys]))
        lines.append(f"### {title}")
        lines.append(g.sort_values(["occurrence","rows_with_subject"], ascending=[False, False]).head(top_n).to_string(index=False))
        lines.append("")
    report_path.write_text("\n".join(lines), encoding="utf-8")
    log.info("Appended per-group summary to report.md and wrote subject_ranking_by_group.csv")


def write_run_info(args, usage_title_col, usage_issn_col, rpt_col, full_df_rows, full_rank_rows):
    """Write run_info.json with args, detected columns, counts, and timestamp."""
    info = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "args": {
            "usage": str(args.usage),
            "kbart": str(args.kbart),
            "kbart_sheet": str(args.kbart_sheet),
            "sample_rows": int(args.sample_rows),
            "out_prefix": str(args.out_prefix),
            "alias_file": str(args.alias_file) if args.alias_file else None,
            "group_by": str(args.group_by) if args.group_by else None,
            "top_n": int(args.top_n),
            "enable_fuzzy": bool(args.enable_fuzzy),
            "fuzzy_threshold": int(args.fuzzy_threshold),
            "fuzzy_max": int(args.fuzzy_max),
            "dry_run": bool(args.dry_run),
            "verbosity": int(args.verbose),
        },
        "detected_columns": {
            "usage_title_col": str(usage_title_col),
            "usage_issn_col": str(usage_issn_col) if usage_issn_col else None,
            "rpt_col": str(rpt_col),
        },
        "counts": {
            "full_df_rows": int(full_df_rows),
            "full_rank_rows": int(full_rank_rows),
        },
    }
    Path("run_info.json").write_text(json.dumps(info, indent=2), encoding="utf-8")
    log.info("Wrote run_info.json")

# ------------------------ Main ------------------------
def fail(msg, code=2):
    log.error(msg)
    sys.exit(code)


def require(cond, msg):
    if not cond:
        fail(msg)


def main():
    args = parse_args()
    setup_logging(args.verbose)
    add_file_logging()  # log to run.log as well as console

    base = Path.cwd()
    usage_path = (base / args.usage).resolve()
    kbart_path = (base / args.kbart).resolve()

    if not usage_path.exists():
        fail(f"usage file not found: {usage_path}")
    if not kbart_path.exists():
        fail(f"KBART file not found: {kbart_path}")

    log.info("Loading usage…")
    usage = load_usage_any(usage_path)

    # Detect key columns in usage
    usage_title_col = find_column(usage, ["Title", "publication_title", "publication title", "title"], must_contain_all=["title"])
    usage_issn_col = find_column(
        usage,
        ["Online_ISSN", "Online ISSN", "eISSN", "ISSN (Online)", "online_identifier", "online identifier"],
        must_contain_all=["online", "issn"],
    ) or find_column(usage, ["eISSN", "EISSN", "e-issn"])
    rpt_col = find_column(
        usage,
        ["Reporting_Period_Total", "Reporting Period Total", "Reporting_Period_Total (RPT)"],
        must_contain_all=["reporting", "period", "total"],
    ) or next((c for c in usage.columns if norm_col(str(c)) == "reporting_period_total"), None)

    require(usage_title_col, "Could not find a Title column in usage file")
    require(rpt_col, "Could not find a Reporting Period Total column in usage file")
    log.info(f"Usage columns -> Title: '{usage_title_col}' | ISSN: '{usage_issn_col or 'N/A'}' | RPT: '{rpt_col}'")

    log.info("Loading KBART…")
    try:
        kbart_df = pd.read_excel(kbart_path, sheet_name=args.kbart_sheet)
    except ValueError as e:
        # Sheet not found; fall back to first sheet
        log.warning(f"Sheet '{args.kbart_sheet}' not found in KBART. Falling back to first sheet. ({e})")
        kbart_df = pd.read_excel(kbart_path)

    issn_col, title_col, subject_cols = build_kbart_lookups(kbart_df)
    kbart_title_tbl, kbart_issn_tbl = build_kbart_subject_table(kbart_df, subject_cols, title_col, issn_col)

    # Load aliases (optional)
    alias_map = load_subject_aliases(args.alias_file)

    # Sample
    log.info("Creating demo sample…")
    demo_df = attach_subjects_vectorized(
        usage.head(args.sample_rows), subject_cols, usage_title_col, usage_issn_col, kbart_title_tbl, kbart_issn_tbl
    )
    # Fuzzy fallback on sample (optional, cheap)
    if args.enable_fuzzy:
        demo_df["__norm_title"] = demo_df[usage_title_col].map(normalize_key)
        demo_df = fuzzy_title_fill(demo_df, kbart_title_tbl, usage_title_col, subject_cols, args.fuzzy_threshold, args.fuzzy_max)
        demo_df.drop(columns=["__norm_title"], errors="ignore", inplace=True)
    # Apply aliases to sample
    apply_subject_aliases_inplace(demo_df, subject_cols, alias_map)
    demo_rank = compute_subject_ranking(demo_df, subject_cols, rpt_col)

    # Full
    log.info("Creating full merged file…")
    full_df = attach_subjects_vectorized(usage, subject_cols, usage_title_col, usage_issn_col, kbart_title_tbl, kbart_issn_tbl)

    # Fuzzy fallback (optional)
    if args.enable_fuzzy:
        full_df["__norm_title"] = full_df[usage_title_col].map(normalize_key)
        full_df = fuzzy_title_fill(full_df, kbart_title_tbl, usage_title_col, subject_cols, args.fuzzy_threshold, args.fuzzy_max)
        full_df.drop(columns=["__norm_title"], errors="ignore", inplace=True)

    # Apply aliases (optional)
    apply_subject_aliases_inplace(full_df, subject_cols, alias_map)

    full_rank = compute_subject_ranking(full_df, subject_cols, rpt_col)

    # Per-group rankings (optional)
    by_group_rank = None
    if args.group_by:
        group_cols = [g.strip() for g in args.group_by.split(",") if g.strip()]
        missing = [g for g in group_cols if g not in full_df.columns]
        if missing:
            log.warning(f"--group-by columns not found: {missing}. Skipping per-group rankings.")
        else:
            by_group_rank = compute_subject_ranking_by_group(full_df, group_cols, subject_cols, rpt_col)

    # Summaries & artifacts
    summarize_matches(full_df, subject_cols, tag="full")
    write_subject_artifacts(full_rank, top_n=args.top_n)
    if by_group_rank is not None and not by_group_rank.empty:
        write_subject_artifacts_by_group(by_group_rank, [g for g in args.group_by.split(",") if g.strip()], top_n=args.top_n)
    write_run_info(args, usage_title_col, usage_issn_col, rpt_col, len(full_df), len(full_rank))

    # Data issues, if any
    if hasattr(full_df, "_issues") and not full_df._issues.empty:
        full_df._issues.to_csv("data_issues.csv", index=False)
        log.info(f"Wrote data_issues.csv with {len(full_df._issues):,} rows")

    # Excel outputs
    if not args.dry_run:
        out_sample = f"{args.out_prefix}_sample_with_subjects.xlsx"
        out_full = f"{args.out_prefix}_full_with_subjects.xlsx"

        # SAMPLE
        with pd.ExcelWriter(out_sample, engine="openpyxl") as w:
            demo_df.to_excel(w, index=False, sheet_name="Data")
            demo_rank.to_excel(w, index=False, sheet_name="Subject Ranking")
            _polish_excel_book(w, ["Data", "Subject Ranking"])

        # FULL
        with pd.ExcelWriter(out_full, engine="openpyxl") as w:
            full_df.to_excel(w, index=False, sheet_name="Data")
            full_rank.to_excel(w, index=False, sheet_name="Subject Ranking")
            _polish_excel_book(w, ["Data", "Subject Ranking"])

        log.info(f"Wrote {out_sample} and {out_full}")


def _polish_excel_book(writer: pd.ExcelWriter, sheets: list[str]):
    """Freeze header row and adjust column widths."""
    for sheet in sheets:
        ws = writer.book[sheet]
        ws.freeze_panes = "A2"  # freeze the first row
        # Best-effort column widths
        for col_cells in ws.columns:
            try:
                max_len = max(len(str(c.value)) if c.value is not None else 0 for c in col_cells)
            except ValueError:
                max_len = 12
            letter = col_cells[0].column_letter
            ws.column_dimensions[letter].width = max(10, min(40, max_len + 2))


if __name__ == "__main__":
    main()
