"""Streamlit app to rank IEEE subjects using usage and KBART data."""

import pandas as pd
import streamlit as st
from io import BytesIO
from pathlib import Path

st.set_page_config(page_title="Subject Ranker", page_icon="📚", layout="wide")
st.title("📚 IEEE Subject Ranker")

st.sidebar.header("Upload files")
usage_file = st.sidebar.file_uploader("Usage file (Excel/CSV)", type=["xlsx", "xls", "csv"])
kbart_file = st.sidebar.file_uploader("KBART file (Excel)", type=["xlsx", "xls"])
show_chart = st.sidebar.checkbox("Show chart", value=True)


def _load_usage(file):
    """Load usage data from an uploaded file."""
    if file is None:
        return None
    ext = Path(file.name).suffix.lower()
    if ext == ".csv":
        return pd.read_csv(file)
    return pd.read_excel(file)


def _load_kbart(file):
    """Load KBART metadata from an uploaded Excel file."""
    if file is None:
        return None
    try:
        return pd.read_excel(file, sheet_name="Kbart Data")
    except ValueError:
        return pd.read_excel(file)


def _norm_issn(val: str) -> str:
    if pd.isna(val):
        return ""
    return str(val).replace("-", "").strip().lower()


def _find_col(df, candidates):
    """Return the first column that contains one of the candidate tokens."""
    norm = {c: c.lower().replace(" ", "") for c in df.columns}
    for cand in candidates:
        c = cand.lower().replace(" ", "")
        for col, n in norm.items():
            if c in n or n in c:
                return col
    return None


def _build_lookup(kbart_df):
    issn_col = _find_col(kbart_df, ["online issn", "online_identifier", "eissn"])
    title_col = _find_col(kbart_df, ["publication title", "title"])
    subj_cols = [c for c in kbart_df.columns if "subject" in str(c).lower()]
    kbart_df["__norm_issn"] = kbart_df[issn_col].map(_norm_issn)
    kbart_df["__norm_title"] = kbart_df[title_col].astype(str).str.casefold()
    kbart_df["__subjects"] = kbart_df[subj_cols].apply(
        lambda r: [s.strip() for s in r if pd.notna(s) and str(s).strip()], axis=1
    )
    issn_map = kbart_df.set_index("__norm_issn")["__subjects"].to_dict()
    title_map = kbart_df.set_index("__norm_title")["__subjects"].to_dict()
    return issn_map, title_map


def _attach_subjects(usage_df, kbart_lookup):
    usage_df = usage_df.copy()
    issn_map, title_map = kbart_lookup
    issn_col = _find_col(usage_df, ["online issn", "eissn", "issn"])
    title_col = _find_col(usage_df, ["title", "publication title"])
    rpt_col = _find_col(usage_df, ["reporting period total", "total"])
    if title_col is None or rpt_col is None:
        return usage_df, None
    usage_df["__norm_issn"] = usage_df[issn_col].map(_norm_issn) if issn_col else ""
    usage_df["__norm_title"] = usage_df[title_col].astype(str).str.casefold()
    def lookup(row):
        subs = issn_map.get(row["__norm_issn"], [])
        if not subs:
            subs = title_map.get(row["__norm_title"], [])
        return subs
    usage_df["subjects"] = usage_df.apply(lookup, axis=1)
    rows = []
    for _, r in usage_df.iterrows():
        for s in r["subjects"]:
            rows.append({"subject": s, "usage": r[rpt_col]})
    rank_df = pd.DataFrame(rows)
    if not rank_df.empty:
        rank_df = rank_df.groupby("subject")["usage"].sum().reset_index().sort_values("usage", ascending=False)
    return usage_df, rank_df


if usage_file and kbart_file:
    usage_df = _load_usage(usage_file)
    kbart_df = _load_kbart(kbart_file)
    lookup = _build_lookup(kbart_df)
    merged_df, ranking = _attach_subjects(usage_df, lookup)
    if ranking is None:
        st.error("Could not determine required columns in the usage file.")
    elif ranking.empty:
        st.warning("No subjects were matched. Check your input files.")
    else:
        st.subheader("Subject Ranking")
        st.dataframe(ranking)
        if show_chart:
            st.bar_chart(ranking.set_index("subject"))
        buf = BytesIO()
        ranking.to_csv(buf, index=False)
        st.download_button("Download ranking CSV", buf.getvalue(), file_name="subject_ranking.csv", mime="text/csv")
else:
    st.info("Upload both a usage file and a KBART file to begin.")
