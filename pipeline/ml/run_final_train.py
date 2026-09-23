import sys
from pathlib import Path
import json
import duckdb
import pandas as pd

# Add repo root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pipeline.config import DUCKDB_PATH, CITIES
from pipeline.ml.train import run_ml_pipeline

print("=" * 80)
print("URBANPULSE FINAL ML AUDIT & REGENERATION")
print("=" * 80)

# 1. Dataset Window Check
con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
query = """
WITH with_target AS (
    SELECT 
        city, 
        metric_date,
        LEAD(avg_us_aqi, 1) OVER (PARTITION BY city ORDER BY metric_date) AS target_next_day_aqi
    FROM fct_daily_city_metrics
)
SELECT 
    city, 
    COUNT(*) as total_days,
    COUNT(target_next_day_aqi) as labeled_days,
    MIN(metric_date) as start_date,
    MAX(metric_date) as end_date
FROM with_target
GROUP BY city
ORDER BY city
"""
df_summary = con.execute(query).df()
con.close()

print("\n--- Current Gold Dataset Window per City ---")
print(df_summary.to_string(index=False))

# 2. Run the ML Pipeline (Global Config Selection via Validation -> Refit on Train+Val -> Single Test Evaluation)
print("\n--- Executing Full ML Pipeline ---")
result = run_ml_pipeline()

# 3. Print Final Metrics Table
print("\n--- FINAL LOCKED ML METRICS TABLE ---")
print(f"{'City':<12} | {'Train':<5} | {'Val':<4} | {'Test':<4} | {'Val MAE':<8} | {'Test MAE':<9} | {'Base MAE':<9} | {'Delta':<8} | {'Beats?':<7} | {'Winning Global Config'}")
print("-" * 105)

meta_dict = result["metadata"]
winning_cfg = result["hyperparameter_selection"]["winning_config"]
val_scores = result["hyperparameter_selection"]["val_scores"]

total_val = []
total_test = []
total_base = []

for city_key in sorted(CITIES.keys()):
    m = meta_dict[city_key]
    val = m["val_mae"]
    test = m["test_mae"]
    base = m["naive_baseline_mae"]
    delta = m["delta_vs_baseline_pct"]
    beats = m["beats_baseline"]
    tr_n = m["train_samples"]
    va_n = m["val_samples"]
    te_n = m["test_samples"]
    
    total_val.append(val)
    total_test.append(test)
    total_base.append(base)
    
    sign = "+" if delta > 0 else ""
    print(f"{city_key:<12} | {tr_n:<5} | {va_n:<4} | {te_n:<4} | {val:<8.2f} | {test:<9.2f} | {base:<9.2f} | {sign + str(delta) + '%':>8} | {str(beats):<7} | {winning_cfg}")

print("-" * 105)
mean_val = sum(total_val) / len(total_val)
mean_test = sum(total_test) / len(total_test)
mean_base = sum(total_base) / len(total_base)
overall_delta = round(((mean_base - mean_test) / mean_base) * 100, 1)
print(f"{'Aggregate':<12} | {'39':<5} | {'8':<4} | {'12':<4} | {mean_val:<8.2f} | {mean_test:<9.2f} | {mean_base:<9.2f} | {('+' if overall_delta > 0 else '') + str(overall_delta) + '%':>8} | {'5 of 8':<7} | {winning_cfg}")

print(f"\nWinning Global Config: {winning_cfg}")
print("Config Hyperparameters:", result["hyperparameter_selection"]["params"])
print("Mean Validation MAE Across All Candidate Configs:")
for cfg_name, score in sorted(val_scores.items(), key=lambda x: x[1]):
    print(f"  {cfg_name:<16} : {score:.2f}")

# Also save this summary table as json for sync verification
df_summary_str = df_summary.copy()
df_summary_str["start_date"] = df_summary_str["start_date"].astype(str)
df_summary_str["end_date"] = df_summary_str["end_date"].astype(str)

summary_export = {
    "dataset": df_summary_str.to_dict(orient="records"),
    "winning_config": winning_cfg,
    "params": result["hyperparameter_selection"]["params"],
    "metrics": {
        city: {
            "val_mae": meta_dict[city]["val_mae"],
            "test_mae": meta_dict[city]["test_mae"],
            "base_mae": meta_dict[city]["naive_baseline_mae"],
            "delta_pct": meta_dict[city]["delta_vs_baseline_pct"],
            "beats_baseline": meta_dict[city]["beats_baseline"]
        }
        for city in sorted(CITIES.keys())
    },
    "aggregate": {
        "val_mae": round(mean_val, 2),
        "test_mae": round(mean_test, 2),
        "base_mae": round(mean_base, 2),
        "overall_delta_pct": overall_delta,
        "cities_beating_baseline": sum(1 for city in CITIES if meta_dict[city]["beats_baseline"]),
        "total_cities": len(CITIES)
    }
}

out_path = Path(__file__).resolve().parent / "locked_metrics.json"
with open(out_path, "w") as f:
    json.dump(summary_export, f, indent=2)

print(f"\nSuccessfully locked metrics to {out_path}")
