import io
import os
import sys
import time
import shutil
import subprocess
import platform
from datetime import datetime
from pathlib import Path

import streamlit as st

# --- Page setup ---
st.set_page_config(
    page_title="IEEE Subject Ranker",
    page_icon="📚",
    layout="wide",
)

# --- Branded header ---
st.markdown("""
<div style="display:flex;align-items:center;gap:12px;margin-bottom:12px;">
  <img src="https://contentonline.com/wp-content/uploads/2025/06/cropped-Hogupplost.jpg"
       alt="Content Online" style="height:40px;">
  <div style="font-size:1.4rem;font-weight:600;">IEEE Subject Ranker</div>
</div>
""", unsafe_allow_html=True)

# --- Styles ---
st.markdown(
    """
    <style>
    .small { font-size: 0.85rem; color: #666; }
    .ok { color: #0a7; font-weight: 600; }
    .warn { color: #c77; font-weight: 600; }
    .muted { color: #888; }
    .foot { font-size: 0.85rem; color: #777; margin-top: 1rem; }
    </style>
    """,
    unsafe_allow_html=True
)

# --- Paths & helpers ---
BASE = Path.cwd()
SCRIPT = BASE / "merge_ieee_subjects.py"
DEFAULT_USAGE = BASE / "usage.xlsx"
DEFAULT_KBART = BASE / "IEEEXplore_Global_IEL.xlsx"

def _decode_best_effort(b: bytes) -> str:
    """
    Decode bytes using a safe, cross-platform strategy:
    try utf-8, utf-8-sig, mbcs (Windows), cp1252, then latin-1,
    and finally utf-8 with replacement to avoid crashes.
    """
    encodings = ["utf-8", "utf-8-sig"]
    if platform.system().lower().startswith("win"):
        encodings.append("mbcs")
    encodings += ["cp1252", "latin-1"]
    for enc in encodings:
        try:
            return b.decode(enc)
        except Exception:
            continue
    return b.decode("utf-8", errors="replace")

def write_uploaded(file_uploader, out_path: Path):
    if file_uploader is None:
        return False
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "wb") as f:
        f.write(file_uploader.getbuffer())
    return True

def list_outputs(run_dir: Path):
    exts = [".xlsx", ".csv", ".md", ".json", ".parquet", ".log"]
    files = []
    for p in sorted(run_dir.glob("*")):
        if p.suffix.lower() in exts:
            files.append(p)
    return files

def download_button_for_file(path: Path, label_prefix="Download"):
    with open(path, "rb") as f:
        b = f.read()
    st.download_button(
        label=f"{label_prefix}: {path.name}",
        data=b,
        file_name=path.name,
        mime="application/octet-stream",
        key=f"dl-{path.name}-{path.stat().st_mtime}"
    )

def which_python():
    return sys.executable or "python"

# --- Sidebar: inputs ---
st.sidebar.header("⚙️ Options")

# File inputs (either choose local existing files or upload)
use_local_usage = st.sidebar.toggle("Use local usage.xlsx in project folder", value=DEFAULT_USAGE.exists())
uploaded_usage = None
if not use_local_usage:
    uploaded_usage = st.sidebar.file_uploader("Upload usage.xlsx / .csv / .parquet", type=["xlsx", "xls", "csv", "parquet"])

use_local_kbart = st.sidebar.toggle("Use local IEEEXplore_Global_IEL.xlsx", value=DEFAULT_KBART.exists())
uploaded_kbart = None
if not use_local_kbart:
    uploaded_kbart = st.sidebar.file_uploader("Upload KBART Excel", type=["xlsx", "xls"])

kbart_sheet = st.sidebar.text_input("KBART sheet name", value="Kbart Data")
sample_rows = st.sidebar.number_input("Sample rows", min_value=0, max_value=1000, value=20, step=10)
out_prefix = st.sidebar.text_input("Output filename prefix", value="usage")

st.sidebar.subheader("Matching & Ranking")
enable_fuzzy = st.sidebar.checkbox("Enable fuzzy title fallback (requires rapidfuzz)", value=False)
fuzzy_threshold = st.sidebar.slider("Fuzzy threshold", min_value=80, max_value=100, value=96)
fuzzy_max = st.sidebar.number_input("Max unmatched titles to fuzz", min_value=100, max_value=20000, value=2000, step=100)
group_by = st.sidebar.text_input("Group by columns (comma-separated)", value="")
top_n = st.sidebar.number_input("Top N in report.md", min_value=5, max_value=100, value=20, step=5)
dry_run = st.sidebar.checkbox("Dry run (don't write Excel files)", value=False)
verbosity = st.sidebar.select_slider("Verbosity", options=["", "-v", "-vv"], value="-v")

st.title("📚 IEEE Subject Ranker — GUI")
st.caption("Merge IEEE KBART subjects into COUNTER usage and build a weighted subject ranking.")

if not SCRIPT.exists():
    st.error("`merge_ieee_subjects.py` not found in this folder. Place the GUI (app.py) next to your script.")
    st.stop()

with st.expander("ℹ️ What this app does", expanded=False):
    st.markdown("""
- Wraps your existing `merge_ieee_subjects.py` and runs it with the options you choose.
- Saves each run to its own folder under `./gui_runs/<timestamp>/`.
- Shows console output, `run.log`, and lets you download generated artifacts.
""")

# --- Main action ---
colL, colR = st.columns([2, 1])
with colL:
    run_it = st.button("▶️ Process", type="primary", use_container_width=True)
with colR:
    clear_btn = st.button("🧹 Clean last GUI run", use_container_width=True)

gui_root = BASE / "gui_runs"
gui_root.mkdir(exist_ok=True)
latest_link = BASE / ".latest_gui_run"

if clear_btn:
    if latest_link.exists():
        try:
            last = Path(latest_link.read_text(encoding="utf-8")).strip()
            last_path = Path(last)
            if last_path.exists() and last_path.is_dir():
                shutil.rmtree(last_path)
                st.success(f"Removed: {last_path}")
            latest_link.unlink(missing_ok=True)
        except Exception as e:
            st.warning(f"Could not clean: {e}")
    else:
        st.info("No remembered last run.")

if run_it:
    # Prepare run directory
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = gui_root / f"run_{ts}"
    run_dir.mkdir(parents=True, exist_ok=True)

    # Resolve usage file
    if use_local_usage:
        if not DEFAULT_USAGE.exists():
            st.error("Local usage.xlsx not found. Uncheck the toggle and upload a file.")
            st.stop()
        usage_path = DEFAULT_USAGE
    else:
        if uploaded_usage is None:
            st.error("Please upload a usage file.")
            st.stop()
        usage_path = run_dir / uploaded_usage.name
        write_uploaded(uploaded_usage, usage_path)

    # Resolve KBART file
    if use_local_kbart:
        if not DEFAULT_KBART.exists():
            st.error("Local IEEEXplore_Global_IEL.xlsx not found. Uncheck the toggle and upload a file.")
            st.stop()
        kbart_path = DEFAULT_KBART
    else:
        if uploaded_kbart is None:
            st.error("Please upload a KBART Excel file.")
            st.stop()
        kbart_path = run_dir / uploaded_kbart.name
        write_uploaded(uploaded_kbart, kbart_path)

    # Build command
    cmd = [
        which_python(),
        str(SCRIPT),
        "--usage", str(usage_path),
        "--kbart", str(kbart_path),
        "--kbart-sheet", kbart_sheet,
        "--sample-rows", str(sample_rows),
        "--out-prefix", out_prefix,
    ]
    if dry_run:
        cmd.append("--dry-run")
    if verbosity:
        cmd.append(verbosity)
    if enable_fuzzy:
        cmd.append("--enable-fuzzy")
        cmd.extend(["--fuzzy-threshold", str(fuzzy_threshold)])
        cmd.extend(["--fuzzy-max", str(fuzzy_max)])
    if group_by.strip():
        cmd.extend(["--group-by", group_by])
    cmd.extend(["--top-n", str(top_n)])

    st.markdown("**Command:**")
    st.code(" ".join(cmd), language="bash")

    # Run process (capture BYTES, then decode robustly)
    with st.status("Running script…", expanded=True) as status:
        try:
            proc = subprocess.run(
                cmd,
                cwd=run_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=False,          # capture bytes
                check=False,
            )
            out_text = _decode_best_effort(proc.stdout or b"")
            st.write("**Console output**")
            st.code(out_text if out_text.strip() else "(no output)", language="bash")
            if proc.returncode == 0:
                st.markdown('<span class="ok">Finished successfully.</span>', unsafe_allow_html=True)
                status.update(label="Done", state="complete")
            else:
                st.markdown(f'<span class="warn">Exited with code {proc.returncode}</span>', unsafe_allow_html=True)
                status.update(label="Completed with errors", state="error")
        except Exception as e:
            st.exception(e)
            st.stop()

    # Remember last run dir
    latest_link.write_text(str(run_dir), encoding="utf-8")

    # Show run.log if present (read as bytes and decode robustly)
    log_path = run_dir / "run.log"
    if log_path.exists():
        st.subheader("Run log")
        try:
            raw = log_path.read_bytes()
            decoded = _decode_best_effort(raw)
            st.code(decoded[-10000:], language="log")
        except Exception:
            st.code("(could not read run.log)")

    # List outputs
    st.subheader("Artifacts")
    outputs = list_outputs(run_dir)
    if not outputs:
        st.info("No artifacts found (check logs above).")
    else:
        for f in outputs:
            download_button_for_file(f, label_prefix="Download")

