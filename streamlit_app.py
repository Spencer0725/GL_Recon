import io
import re
import requests
import pandas as pd
import streamlit as st
from datetime import datetime

st.set_page_config(page_title="GL Reconciliation Checker", layout="wide")

st.title("GL Reconciliation Checker (Upload Excel/CSV or Google Sheets)")
st.caption("Upload your GL reconciliation file(s) or paste a Google Sheets URL. Configure criteria, run checks, and download a Results workbook.")

# --- Criteria (sidebar) ---
with st.sidebar:
    st.header("Criteria")
    timeliness_sla_days = st.number_input("Timeliness SLA (days)", min_value=0, value=5)
    tieout_tolerance_abs = st.number_input("Tie-out absolute tolerance ($)", min_value=0.0, value=1000.0, step=100.0)
    tieout_tolerance_pct = st.number_input("Tie-out % of GL (as fraction)", min_value=0.0, value=0.002, step=0.001, format="%.4f")
    require_sod = st.checkbox("Require preparer ≠ approver (SoD)", value=True)
    allow_items_over_threshold_with_plan = st.checkbox("Allow aged items if action plan present", value=True)
    aging_threshold_days = st.number_input("Aging threshold (days)", min_value=0, value=60)

crit = {
    "timeliness_sla_days": int(timeliness_sla_days),
    "tieout_tolerance_abs": float(tieout_tolerance_abs),
    "tieout_tolerance_pct": float(tieout_tolerance_pct),
    "require_sod": bool(require_sod),
    "allow_items_over_threshold_with_plan": bool(allow_items_over_threshold_with_plan),
    "aging_threshold_days": int(aging_threshold_days),
}

# --- Upload area ---
st.subheader("1) Upload Excel/CSV and/or paste Google Sheets URL")
uploaded_files = st.file_uploader("Upload one or more files (.xlsx or .csv)", type=["xlsx", "csv"], accept_multiple_files=True)
st.markdown("Or paste a Google Sheets link (anyone with link or your org can view):")
gsheet_url = st.text_input("Google Sheets URL (optional)")

# --- Column mapping (optional) ---
with st.expander("Optional: Map your column names (if different from defaults)"):
    st.write("Map your columns to the app's expected names. Leave defaults if your headers match.")
    defaults = {
        "entity":"entity",
        "account_id":"account_id",
        "account_name":"account_name",
        "period_start_date":"period_start_date",
        "period_end_date":"period_end_date",
        "gl_ending_balance":"gl_ending_balance",
        "subledger_ending_balance":"subledger_ending_balance",
        "preparer":"preparer",
        "prepared_on":"prepared_on",
        "approver":"approver",
        "approved_on":"approved_on",
        "reconciling_items_count":"reconciling_items_count",
        "items_over_aging_threshold":"items_over_aging_threshold",
        "action_plan_present":"action_plan_present",
        "documentation_links":"documentation_links"
    }
    colmap = {k: st.text_input(k, v) for k, v in defaults.items()}
STD_COLS = list(defaults.keys())

def normalize(df, colmap):
    df = df.copy()
    for std, src in colmap.items():
        if src in df.columns:
            df.rename(columns={src: std}, inplace=True)
        else:
            df[std] = None
    return df[STD_COLS].copy()

def parse_date_safe(x):
    if pd.isna(x) or x is None:
        return None
    try:
        return pd.to_datetime(x).date()
    except Exception:
        return None

def to_bool(x):
    if isinstance(x, str):
        return x.strip().lower() in ["y","yes","true","1"]
    if isinstance(x, (int, float)):
        return bool(x)
    return False

def evaluate(df, crit, checklist_version="v1"):
    results = []
    run_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    for _, r in df.iterrows():
        entity = r.get("entity")
        account_id = r.get("account_id")
        account_name = r.get("account_name")
        period_end = parse_date_safe(r.get("period_end_date"))
        gl_bal = float(r.get("gl_ending_balance") or 0.0)
        sub_bal = float(r.get("subledger_ending_balance") or 0.0)
        preparer = str(r.get("preparer") or "")
        approver = str(r.get("approver") or "")
        approved_on = parse_date_safe(r.get("approved_on"))
        items_over = int(r.get("items_over_aging_threshold") or 0)
        plan = to_bool(r.get("action_plan_present"))
        evidence = r.get("documentation_links") or ""

        variance = gl_bal - sub_bal
        tol_abs = float(crit.get("tieout_tolerance_abs", 0.0))
        tol_pct = float(crit.get("tieout_tolerance_pct", 0.0))
        tol = max(tol_abs, abs(gl_bal) * tol_pct)
        tieout_pass = abs(variance) <= tol

        sod_required = bool(crit.get("require_sod", True))
        sod_pass = (preparer != approver) if sod_required else True

        sla_days = int(crit.get("timeliness_sla_days", 5))
        sla_over = None
        timely_pass = True
        if approved_on and period_end:
            delta_days = (approved_on - period_end).days
            sla_over = max(0, delta_days - sla_days)
            timely_pass = delta_days <= sla_days

        allow_over_with_plan = bool(crit.get("allow_items_over_threshold_with_plan", True))
        aging_pass = (items_over == 0) or (allow_over_with_plan and plan)

        failures, severity = [], "low"
        if not tieout_pass:
            failures.append(f"Tie-out variance {variance:,.2f} exceeds tolerance {tol:,.2f}")
            severity = "high"
        if not sod_pass:
            failures.append("Segregation of duties failed (preparer equals approver)")
            severity = "high"
        if not timely_pass:
            failures.append(f"Approval exceeded SLA by {sla_over} day(s)")
            severity = "medium" if severity != "high" else "high"
        if not aging_pass:
            failures.append("Aged items without action plan")
            severity = "medium" if severity == "low" else severity

        status = "pass" if not failures else ("fail" if severity == "high" else "warn")
        rationale = " | ".join(failures) if failures else "All checks passed within thresholds."

        results.append({
            "entity": entity,
            "account_id": account_id,
            "account_name": account_name,
            "period_end_date": period_end,
            "status": status,
            "severity": severity,
            "rationale": rationale,
            "variance_amount": variance,
            "sla_days_over": sla_over if sla_over is not None else 0,
            "sod_violation": (not sod_pass),
            "aged_items_flag": (not aging_pass),
            "evidence_link": evidence
        })
    return pd.DataFrame(results)

def read_uploaded(files):
    frames = []
    for f in files:
        name = f.name.lower()
        try:
            if name.endswith(".csv"):
                df = pd.read_csv(f, encoding="utf-8", engine="python", on_bad_lines="skip")
            else:
                df = pd.read_excel(f)
            frames.append(df)
        except Exception as e:
            st.warning(f"Skipping {f.name}: {e}")
    return frames

def gsheet_to_csv_export(url: str):
    m = re.match(r".*spreadsheets/d/([a-zA-Z0-9-_]+).*?(?:gid=([0-9]+))?.*", url)
    if not m:
        return None
    sheet_id = m.group(1)
    gid = m.group(2) or "0"
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"

def read_gsheet(url):
    export = gsheet_to_csv_export(url)
    if not export:
        st.error("Could not parse Google Sheets URL. Please ensure it’s a standard Sheets link or upload as Excel/CSV.")
        return None
    try:
        resp = requests.get(export, timeout=15)
        resp.raise_for_status()
        df = pd.read_csv(io.BytesIO(resp.content))
        return df
    except Exception as e:
        st.error(f"Failed to fetch Google Sheet: {e}")
        return None

run = st.button("2) Run checks")

if run:
    frames = []
    if uploaded_files:
        frames.extend(read_uploaded(uploaded_files))
    if gsheet_url.strip():
        df_gs = read_gsheet(gsheet_url.strip())
        if df_gs is not None:
            frames.append(df_gs)

    if not frames:
        st.warning("No data found. Upload a file or provide a valid Google Sheets URL.")
    else:
        dfs_norm = [normalize(df, colmap) for df in frames]
        df_all = pd.concat(dfs_norm, ignore_index=True)
        df_results = evaluate(df_all, crit)

        st.success("Checks complete.")
        st.subheader("Results preview")
        st.dataframe(df_results.head(100))

        ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        excel_name = f"gl_recon_results_{ts}.xlsx"
        out = io.BytesIO()
        with pd.ExcelWriter(out, engine="xlsxwriter", datetime_format="yyyy-mm-dd") as writer:
            df_all.to_excel(writer, sheet_name="GL_Recon_Input", index=False)
            pd.DataFrame([crit]).to_excel(writer, sheet_name="Criteria", index=False)
            df_results.to_excel(writer, sheet_name="Results", index=False)
        out.seek(0)
        st.download_button("Download Results Excel", data=out, file_name=excel_name)

st.markdown("---")
st.caption("Expected columns (or map yours): entity, account_id, account_name, period_start_date, period_end_date, gl_ending_balance, subledger_ending_balance, preparer, prepared_on, approver, approved_on, reconciling_items_count, items_over_aging_threshold, action_plan_present, documentation_links.")
