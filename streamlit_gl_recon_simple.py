
import os
import sys
import json
import glob
import pandas as pd
from datetime import datetime

STD_COLS = [
    "entity","account_id","account_name","period_start_date","period_end_date",
    "gl_ending_balance","subledger_ending_balance",
    "preparer","prepared_on","approver","approved_on",
    "reconciling_items_count","items_over_aging_threshold","action_plan_present",
    "documentation_links"
]

def read_any(path):
    path = str(path)
    if path.lower().endswith(".csv"):
        return pd.read_csv(path)
    else:
        return pd.read_excel(path)

def collect_inputs(inputs):
    files = []
    for p in inputs:
        if os.path.isdir(p):
            files.extend(glob.glob(os.path.join(p, "*.xlsx")))
            files.extend(glob.glob(os.path.join(p, "*.csv")))
        else:
            files.append(p)
    return files

def normalize_columns(df, colmap):
    # Create missing standard columns with NaN if not present
    for std in STD_COLS:
        src = colmap.get(std, std)
        if src in df.columns:
            df.rename(columns={src: std}, inplace=True)
        else:
            df[std] = None
    # Keep only standard columns in expected order
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

def load_json_or_default(path, default_obj):
    if not path:
        return default_obj
    with open(path, "r") as f:
        return json.load(f)

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
        prepared_on = parse_date_safe(r.get("prepared_on"))
        approved_on = parse_date_safe(r.get("approved_on"))
        items_over = int(r.get("items_over_aging_threshold") or 0)
        plan = to_bool(r.get("action_plan_present"))
        evidence = r.get("documentation_links") or ""

        # Tie-out
        variance = gl_bal - sub_bal
        tol_abs = float(crit.get("tieout_tolerance_abs", 0.0))
        tol_pct = float(crit.get("tieout_tolerance_pct", 0.0))
        tol = max(tol_abs, abs(gl_bal) * tol_pct)
        tieout_pass = abs(variance) <= tol

        # SoD
        sod_required = bool(crit.get("require_sod", True))
        sod_pass = (preparer != approver) if sod_required else True

        # Timeliness
        sla_days = int(crit.get("timeliness_sla_days", 5))
        sla_over = None
        timely_pass = True
        if approved_on and period_end:
            delta_days = (approved_on - period_end).days
            sla_over = max(0, delta_days - sla_days)
            timely_pass = delta_days <= sla_days

        # Aging (summary-level)
        allow_over_with_plan = bool(crit.get("allow_items_over_threshold_with_plan", True))
        aging_pass = (items_over == 0) or (allow_over_with_plan and plan)

        # Aggregate
        failures = []
        severity = "low"
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
            "evidence_link": evidence,
            "agent_run_id": run_id,
            "checklist_version": checklist_version
        })
    return pd.DataFrame(results)

def main():
    import argparse
    parser = argparse.ArgumentParser(description="GL Reconciliation Agent")
    parser.add_argument("--inputs", nargs="+", required=True, help="Paths to files or folders (.xlsx/.csv)")
    parser.add_argument("--criteria", default=None, help="Path to criteria JSON (optional)")
    parser.add_argument("--column_map", default=None, help="Path to column map JSON (optional)")
    parser.add_argument("--output_excel", default="gl_recon_results.xlsx", help="Output Excel path")
    parser.add_argument("--output_csv", default="gl_recon_results.csv", help="Output CSV path")
    parser.add_argument("--checklist_version", default="v1", help="Version label")
    args = parser.parse_args()

    crit = load_json_or_default(args.criteria, {
        "timeliness_sla_days": 5,
        "tieout_tolerance_abs": 1000.0,
        "tieout_tolerance_pct": 0.002,
        "require_sod": True,
        "allow_items_over_threshold_with_plan": True,
        "aging_threshold_days": 60
    })
    colmap = load_json_or_default(args.column_map, {c:c for c in STD_COLS})

    files = collect_inputs(args.inputs)
    if not files:
        print("No files found in inputs.")
        sys.exit(1)

    frames = []
    for f in files:
        try:
            df = read_any(f)
            df = normalize_columns(df, colmap.copy())
            frames.append(df)
        except Exception as e:
            print(f"Skipping {f}: {e}")

    if not frames:
        print("No readable files after parsing.")
        sys.exit(1)

    df_all = pd.concat(frames, ignore_index=True)
    df_results = evaluate(df_all, crit, checklist_version=args.checklist_version)

    # Write outputs
    with pd.ExcelWriter(args.output_excel, engine="xlsxwriter", datetime_format="yyyy-mm-dd") as writer:
        df_all.to_excel(writer, sheet_name="GL_Recon_Input", index=False)
        pd.DataFrame([crit]).to_excel(writer, sheet_name="Criteria", index=False)
        df_results.to_excel(writer, sheet_name="Results", index=False)
    df_results.to_csv(args.output_csv, index=False)

    print(f"Wrote results to {args.output_excel} and {args.output_csv}")

if __name__ == "__main__":
    main()
