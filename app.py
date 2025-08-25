# app.py
import re
import pdfplumber
import io
import tempfile
from datetime import datetime

import streamlit as st
import pandas as pd

# PDF engines
#import fitz  # PyMuPDF
import camelot

# -------------------------------
# Patterns & Config
# -------------------------------

HEADER_PATTERNS = {
    "plan_period": [
        re.compile(r"Plan\s*Period\s*[:\-]\s*([^\n\r]+)", re.IGNORECASE),
        re.compile(r"Period\s*[:\-]\s*([^\n\r]+)", re.IGNORECASE),
        re.compile(r"Plan\s*Window\s*[:\-]\s*([^\n\r]+)", re.IGNORECASE),
    ],
    "po_number": [
        re.compile(r"\bPO\s*Number\s*[:\-]\s*([0-9\-]+)", re.IGNORECASE),
        re.compile(r"\bPO\s*#\s*[:\-]?\s*([0-9\-]+)", re.IGNORECASE),
        re.compile(r"\bPurchase\s*Order\s*(?:Number)?\s*[:\-]\s*([0-9\-]+)", re.IGNORECASE),
        re.compile(r"\bPO\s*[:\-]\s*([0-9\-]+)", re.IGNORECASE),
    ],
    "partner": [
        re.compile(r"Partner\s*Legal\s*Name\s*[:\-]\s*([^\n\r]+)", re.IGNORECASE),
        re.compile(r"Partner\s*Name\s*[:\-]\s*([^\n\r]+)", re.IGNORECASE),
        re.compile(r"Partner\s*[:\-]\s*([^\n\r]+)", re.IGNORECASE),
        re.compile(r"Reseller\s*[:\-]\s*([^\n\r]+)", re.IGNORECASE),
    ],
}

def _first_match(text: str, pats: list[re.Pattern]) -> str:
    for p in pats:
        m = p.search(text)
        if m:
            return re.sub(r"\s+", " ", m.group(1)).strip()
    return ""

def extract_headers(pdf_path: str) -> dict:
    text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages[:2]:
            text += (page.extract_text() or "") + "\n"
    return {
        "Partner":     _first_match(text, HEADER_PATTERNS["partner"]),
        "PO Number":   _first_match(text, HEADER_PATTERNS["po_number"]),
        "Plan Period": _first_match(text, HEADER_PATTERNS["plan_period"]),
    }


# Acceptable synonyms for the table columns
COL_SYNONYMS = {
    "activity": {"activity", "activitiy", "acti vity", "actvity"},
    "description": {"description", "descr", "desription"},
    "amount": {
        "up to amount (usd)", "up to amount", "amount (usd)", "amount",
        "budget (usd)", "budget", "total amount (usd)", "max amount (usd)"
    },
}

# -------------------------------
# Helpers
# -------------------------------
def normalize_header(text: str) -> str:
    return re.sub(r"\s+", " ", str(text)).strip().lower()

def truncate(text: str, length: int = 100) -> str:
    if text is None:
        return ""
    s = str(text)
    return s[:length].strip()

def clean_amount(x):
    s = str(x or "").strip()
    s = s.replace("\u00A0", " ")
    s = re.sub(r"(?i)\bUSD\b", "", s)
    s = s.replace("$", "").replace(",", "").strip()

    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1].strip()

    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if not m:
        return ""

    num = m.group(0)
    if neg and not num.startswith("-"):
        num = "-" + num
    if re.match(r"^-?\d+\.0+$", num):
        num = str(int(float(num)))
    return num

def _pick_column_indices(df: pd.DataFrame):
    # try to find the header row
    header_row_idx = 0
    for i in range(min(20, len(df))):
        joined = " ".join([str(x or "").strip() for x in df.iloc[i].tolist()])
        if re.search(r"activ", joined, re.IGNORECASE) and re.search(r"descr", joined, re.IGNORECASE):
            header_row_idx = i
            break

    header = df.iloc[header_row_idx].fillna("").astype(str).map(normalize_header).tolist()
    data = df.iloc[header_row_idx+1:].reset_index(drop=True).copy()

    col_map = {"activity": None, "description": None, "amount": None}
    for idx, col in enumerate(header):
        col_clean = normalize_header(col)
        for role, synonyms in COL_SYNONYMS.items():
            if any(s in col_clean for s in synonyms):
                if col_map[role] is None:
                    col_map[role] = idx

    # heuristic for amount if not found
    if col_map["amount"] is None and len(data) > 0:
        cand_scores = []
        for j in range(len(header)):
            series = data.iloc[:, j].astype(str)
            score = series.str.contains(r"\$\d{1,3}(?:,\d{3})+(?:\.\d+)?", regex=True).sum()
            cand_scores.append((score, j))
        cand_scores.sort(reverse=True)
        if cand_scores and cand_scores[0][0] > 0:
            col_map["amount"] = cand_scores[0][1]
    return data, col_map

def extract_table_records(pdf_path: str, plan_period: str, stream_line_scale: int = 15):
    """
    Returns list of dicts with keys: Description, Amount
    Using Camelot 'stream' flavor (no Ghostscript dependency).
    """
    # Try only stream flavor to avoid Ghostscript requirement on the cloud
    tables = []
    try:
        t = camelot.read_pdf(
            pdf_path,
            pages="all",
            flavor="stream",
            line_scale=stream_line_scale,
            strip_text="\n"
        )
        if t.n > 0:
            tables = t
    except Exception:
        pass

    records = []
    last_col_map = None

    for tb in tables:
        df = tb.df
        if df.empty or df.shape[1] < 2:
            continue

        data, col_map = _pick_column_indices(df)
        a, d, m = col_map["activity"], col_map["description"], col_map["amount"]

        # continued table without header: reuse previous map
        if (a is None or d is None) and last_col_map is not None:
            a, d, m = last_col_map.get("activity"), last_col_map.get("description"), last_col_map.get("amount")
            data = df.reset_index(drop=True).copy()

        if a is None or d is None:
            last_col_map = None
            continue

        for _, row in data.iterrows():
            activity = str(row.iloc[a] if a is not None and a < len(row) else "").strip()
            descr = str(row.iloc[d] if d is not None and d < len(row) else "").strip()
            if not activity and not descr:
                continue

            joined = f"{activity} {descr}".strip().lower()
            if joined.startswith("activity") or joined.startswith("description"):
                continue

            description = f"{truncate(activity)} - {truncate(descr)} - {plan_period}"
            amount_raw = clean_amount(row.iloc[m] if m is not None and m < len(row) else "")
            records.append({"Description": description, "Amount": amount_raw})

        last_col_map = {"activity": a, "description": d, "amount": m}

    return records

def extract(pdf_path: str, stream_line_scale: int = 15):
    headers = extract_headers(pdf_path)
    po = headers.get("po_number", "")
    period = headers.get("plan_period", "")
    partner = headers.get("partner", "")
    rows = extract_table_records(pdf_path, period, stream_line_scale=stream_line_scale)
    return po, period, partner, rows

def df_to_excel_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="MDF Data")
    buf.seek(0)
    return buf.read()

# -------------------------------
# Streamlit UI
# -------------------------------
st.set_page_config(page_title="MDF PDF → CSV/Excel", page_icon="📄", layout="wide")
st.title("📄 MDF Agreement PDF → CSV/Excel (No Install)")

st.markdown("""
Upload an MDF Agreement PDF. The app will extract:
- **PO Number**, **Plan Period**, **Partner** (from header)
- **Activity rows** with **Description** and **Amount** (from tables)

**Note:** This app uses Camelot in *stream* mode (no Ghostscript). If a PDF is a scanned image or has complex tables, results may vary.
""")

with st.sidebar:
    st.header("Settings")
    stream_line_scale = st.slider(
        "Table detection line scale (stream flavor)", min_value=5, max_value=60, value=15, step=1,
        help="If rows merge or split oddly, try adjusting this and re-process."
    )

uploaded = st.file_uploader("Upload MDF Agreement PDF", type=["pdf"])

if uploaded:
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(uploaded.read())
        tmp_path = tmp.name

    if st.button("Process PDF"):
        with st.spinner("Extracting data..."):
            po, period, partner, rows = extract(tmp_path, stream_line_scale=stream_line_scale)
            df = pd.DataFrame([
                {
                    "PO Number": po,
                    "Partner": partner,
                    "Description": r["Description"],
                    "Amount": r["Amount"]
                } for r in rows
            ])

        st.success("Done!")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("PO Number", po or "—")
        with col2:
            st.metric("Plan Period", period or "—")
        with col3:
            st.metric("Partner", partner or "—")

        st.subheader("Preview")
        if df.empty:
            st.info("No rows extracted. Try tweaking the line scale in the sidebar or verify the PDF tables are text-based (not scanned images).")
        else:
            st.dataframe(df, use_container_width=True, height=400)

            # Downloads
            csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "⬇️ Download CSV",
                data=csv_bytes,
                file_name=f"mdf_{po or 'output'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )

            xlsx_bytes = df_to_excel_bytes(df)
            st.download_button(
                "⬇️ Download Excel",
                data=xlsx_bytes,
                file_name=f"mdf_{po or 'output'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

    st.caption("If processing errors occur, reduce PDF file size, or re-export as a standard PDF (text-based).")
else:
    st.info("Drag & drop a PDF above and click **Process PDF**.")
