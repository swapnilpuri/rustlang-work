"""
Insert synthetic data into BALANCE_FORECAST_MODEL table.
Usage: python insert_balance_forecast.py <num_records>
"""

import sys
import random
import argparse
from datetime import date, timedelta

import oracledb

USERNAME = "appuser"
PASSWORD = "appuserpass"
CONN_STR = "//localhost:1521/FREEPDB1"

ENTITIES        = [f"ENT{str(i).zfill(4)}" for i in range(1, 21)]
BUSINESS_UNITS  = ["RETAIL", "CORPORATE", "INVESTMENT", "SME", "WEALTH_MGMT"]
PRODUCT_TYPES   = ["LOAN", "DEPOSIT", "CREDIT_CARD", "MORTGAGE", "BOND", "DERIVATIVE"]
PRODUCT_SUBTYPE = {
    "LOAN":        ["PERSONAL", "AUTO", "STUDENT", "COMMERCIAL"],
    "DEPOSIT":     ["SAVINGS", "CURRENT", "TERM", "CALL"],
    "CREDIT_CARD": ["STANDARD", "PREMIUM", "CORPORATE", "SECURED"],
    "MORTGAGE":    ["FIXED", "VARIABLE", "INTEREST_ONLY", "REVERSE"],
    "BOND":        ["SOVEREIGN", "CORPORATE", "MUNICIPAL", "HY"],
    "DERIVATIVE":  ["IRS", "CDS", "FX_FORWARD", "OPTION"],
}
CURRENCIES      = ["USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "SGD"]
SEGMENTS        = ["RETAIL", "MASS_AFFLUENT", "HNW", "UHNW", "SME", "LARGE_CORP", "SOVEREIGN"]
RISK_BUCKETS    = ["AAA", "AA", "A", "BBB", "BB", "B", "CCC", "DEFAULT"]
SCENARIOS       = ["BASE", "STRESS_MILD", "STRESS_SEVERE", "OPTIMISTIC", "REGULATORY"]
SOURCES         = ["CORE_BANKING", "RISK_ENGINE", "ALM_SYSTEM", "CRM", "MANUAL"]
MODEL_NAMES     = ["LGD_MODEL_v2", "PD_LOGIT_v3", "CASHFLOW_DCF", "RUNOFF_REGR", "NIM_FACTOR"]
REPRICING       = ["OVERNIGHT", "1M", "3M", "6M", "1Y", "2Y", "5Y", "FIXED"]
USERS           = ["batch_job", "risk_analyst", "alm_team", "model_owner", "sys_load"]


def random_date(start: date, end: date) -> date:
    return start + timedelta(days=random.randint(0, (end - start).days))


def generate_row(run_id: str) -> dict:
    product_type = random.choice(PRODUCT_TYPES)
    subtype      = random.choice(PRODUCT_SUBTYPE[product_type])
    t0           = round(random.uniform(1_000, 50_000_000), 2)
    # Each Tn drifts slightly from the previous bucket
    balances = [t0]
    for _ in range(36):
        drift = random.uniform(-0.05, 0.05)
        next_val = max(0.0, round(balances[-1] * (1 + drift), 2))
        balances.append(next_val)

    now = date.today()
    created = random_date(date(2022, 1, 1), now)
    updated = random_date(created, now)

    row = {
        "ENTITY_ID":       random.choice(ENTITIES),
        "BUSINESS_UNIT":   random.choice(BUSINESS_UNITS),
        "PRODUCT_TYPE":    product_type,
        "PRODUCT_SUBTYPE": subtype,
        "CURRENCY_CODE":   random.choice(CURRENCIES),
        "CUSTOMER_SEGMENT":random.choice(SEGMENTS),
        "RISK_BUCKET":     random.choice(RISK_BUCKETS),
        "PORTFOLIO_ID":    f"PORT{random.randint(100, 999)}",
        "AS_OF_DATE":      random_date(date(2023, 1, 1), now),
        "SCENARIO_ID":     random.choice(SCENARIOS),
        "VERSION_NO":      random.randint(1, 10),
        # Balance buckets T0–T36
        **{f"T{i}": balances[i] for i in range(37)},
        # Risk metrics
        "EAD":             round(random.uniform(500, 10_000_000), 2),
        "AVG_BALANCE":     round(sum(balances) / len(balances), 2),
        "PEAK_BALANCE":    round(max(balances), 2),
        "PD":              round(random.uniform(0.0001, 0.25), 6),
        "LGD":             round(random.uniform(0.05, 0.85), 6),
        "EXPECTED_LOSS":   round(random.uniform(0, 500_000), 2),
        # ALM / Liquidity
        "RUNOFF_RATE":     round(random.uniform(0.001, 0.20), 6),
        "PREPAYMENT_RATE": round(random.uniform(0.0, 0.15), 6),
        "DURATION_MONTHS": round(random.uniform(1, 360), 2),
        "REPRICING_BUCKET":random.choice(REPRICING),
        # Yield
        "INTEREST_RATE":   round(random.uniform(0.001, 0.15), 6),
        "NIM":             round(random.uniform(-0.02, 0.08), 6),
        "FTP_RATE":        round(random.uniform(0.001, 0.12), 6),
        # Audit
        "MODEL_NAME":      random.choice(MODEL_NAMES),
        "MODEL_RUN_ID":    run_id,
        "SOURCE_SYSTEM":   random.choice(SOURCES),
        "CREATED_DATE":    created,
        "CREATED_BY":      random.choice(USERS),
        "UPDATED_DATE":    updated,
        "UPDATED_BY":      random.choice(USERS),
    }
    return row


INSERT_SQL = """
INSERT INTO BALANCE_FORECAST_MODEL (
    ENTITY_ID, BUSINESS_UNIT, PRODUCT_TYPE, PRODUCT_SUBTYPE, CURRENCY_CODE,
    CUSTOMER_SEGMENT, RISK_BUCKET, PORTFOLIO_ID, AS_OF_DATE, SCENARIO_ID, VERSION_NO,
    T0,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13,T14,T15,T16,T17,T18,
    T19,T20,T21,T22,T23,T24,T25,T26,T27,T28,T29,T30,T31,T32,T33,T34,T35,T36,
    EAD, AVG_BALANCE, PEAK_BALANCE, PD, LGD, EXPECTED_LOSS,
    RUNOFF_RATE, PREPAYMENT_RATE, DURATION_MONTHS, REPRICING_BUCKET,
    INTEREST_RATE, NIM, FTP_RATE,
    MODEL_NAME, MODEL_RUN_ID, SOURCE_SYSTEM,
    CREATED_DATE, CREATED_BY, UPDATED_DATE, UPDATED_BY
) VALUES (
    :ENTITY_ID, :BUSINESS_UNIT, :PRODUCT_TYPE, :PRODUCT_SUBTYPE, :CURRENCY_CODE,
    :CUSTOMER_SEGMENT, :RISK_BUCKET, :PORTFOLIO_ID, :AS_OF_DATE, :SCENARIO_ID, :VERSION_NO,
    :T0,:T1,:T2,:T3,:T4,:T5,:T6,:T7,:T8,:T9,:T10,:T11,:T12,:T13,:T14,:T15,:T16,:T17,:T18,
    :T19,:T20,:T21,:T22,:T23,:T24,:T25,:T26,:T27,:T28,:T29,:T30,:T31,:T32,:T33,:T34,:T35,:T36,
    :EAD, :AVG_BALANCE, :PEAK_BALANCE, :PD, :LGD, :EXPECTED_LOSS,
    :RUNOFF_RATE, :PREPAYMENT_RATE, :DURATION_MONTHS, :REPRICING_BUCKET,
    :INTEREST_RATE, :NIM, :FTP_RATE,
    :MODEL_NAME, :MODEL_RUN_ID, :SOURCE_SYSTEM,
    :CREATED_DATE, :CREATED_BY, :UPDATED_DATE, :UPDATED_BY
)
"""

BATCH_SIZE = 500


def main():
    parser = argparse.ArgumentParser(description="Insert synthetic rows into BALANCE_FORECAST_MODEL")
    parser.add_argument("num_records", type=int, help="Number of records to insert")
    args = parser.parse_args()

    if args.num_records <= 0:
        print("num_records must be a positive integer.")
        sys.exit(1)

    run_id = f"RUN_{date.today().strftime('%Y%m%d')}_{random.randint(1000, 9999)}"
    print(f"Connecting to Oracle: {CONN_STR}")

    with oracledb.connect(user=USERNAME, password=PASSWORD, dsn=CONN_STR) as conn:
        with conn.cursor() as cur:
            total    = args.num_records
            inserted = 0
            while inserted < total:
                batch_count = min(BATCH_SIZE, total - inserted)
                rows = [generate_row(run_id) for _ in range(batch_count)]
                cur.executemany(INSERT_SQL, rows)
                conn.commit()
                inserted += batch_count
                print(f"  Inserted {inserted}/{total} rows...")

    print(f"Done. {total} records inserted with MODEL_RUN_ID={run_id}")


if __name__ == "__main__":
    main()
