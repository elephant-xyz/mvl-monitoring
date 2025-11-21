import argparse
import csv
import time
from datetime import datetime, UTC
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import yaml
import boto3
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import seaborn as sns

# ---------- CONFIG ----------
ACCOUNTS_FILE = "accounts-dev.yaml"
REGION = "us-east-1"  # adjust to your region
STACK_NAME = "elephant-oracle-node"
LOG_GROUP_OUTPUT_KEY = "WorkflowMirrorValidatorLogGroupName"

# CloudWatch Logs Insights query
QUERY = r"""
fields
    message.county as county,
    message.global_completeness as completeness,
    message.msg as msg
| filter msg = "mvl_completeness_metric"
| stats avg(completeness) as avg_global_completeness by county
"""


# ---------- HELPERS ----------


def load_accounts(path: str):
    """
    Load accounts from accounts.yaml.
    Expected YAML structure:
    - Account ID: 123456789012
      username: ...
      password: ...
      aws_access_key_id: ...
      aws_secret_access_key: ...
    """
    with open(path, "r") as f:
        data = yaml.safe_load(f)

    accounts = []
    for item in data:
        accounts.append(
            {
                "account_id": str(item.get("Account ID")),
                "aws_access_key_id": item["aws_access_key_id"],
                "aws_secret_access_key": item["aws_secret_access_key"],
            }
        )
    return accounts


def get_log_group_name(session: boto3.Session, region: str, stack_name: str, output_key: str) -> str | None:
    """
    Look up the CloudFormation stack and extract the log group name
    from the given output key.
    """
    cf = session.client("cloudformation", region_name=region)
    try:
        resp = cf.describe_stacks(StackName=stack_name)
    except cf.exceptions.ClientError as e:
        print(f"[WARN] Failed to describe stack {stack_name}: {e}")
        return None

    stacks = resp.get("Stacks", [])
    if not stacks:
        print(f"[WARN] No stacks found with name {stack_name}")
        return None

    outputs = stacks[0].get("Outputs", []) or []
    for out in outputs:
        if out.get("OutputKey") == output_key:
            return out.get("OutputValue")

    print(f"[WARN] Output key {output_key} not found in stack {stack_name}")
    return None


def run_insights_query(logs_client, log_group_name: str, start_ts: int, end_ts: int):
    """
    Run a Logs Insights query for the given time window (Unix seconds).
    Returns the `results` list from get_query_results.
    """
    try:
        start_resp = logs_client.start_query(
            logGroupName=log_group_name,
            startTime=start_ts,  # Unix time in seconds
            endTime=end_ts,  # Unix time in seconds
            queryString=QUERY,
            limit=10000,
        )
    except logs_client.exceptions.ClientError as e:
        print(f"[WARN] Failed to start query for {log_group_name}: {e}")
        return []

    query_id = start_resp["queryId"]

    while True:
        resp = logs_client.get_query_results(queryId=query_id)
        status = resp["status"]
        if status in ("Complete", "Failed", "Cancelled"):
            if status != "Complete":
                print(f"[WARN] Query {query_id} ended with status {status}")
            return resp.get("results", [])
        time.sleep(1)


def ts_to_iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, UTC).isoformat().replace("+00:00", "Z")


def create_visualization(csv_filepath: str, output_filepath: str):
    """
    Create time series visualization from the metrics CSV file.
    Shows one line per (Account + County) combination.
    """
    # Load data
    df = pd.read_csv(csv_filepath)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Sort by timestamp for proper time series plotting
    df = df.sort_values("timestamp")

    # Set seaborn style for better aesthetics
    sns.set_style("whitegrid")
    sns.set_palette("husl")

    # Create figure
    fig, ax = plt.figure(figsize=(16, 8)), plt.gca()

    # Create a unique identifier for each account+county combination
    df["account_county"] = df["account_id"].astype(str) + " - " + df["county"]

    # Plot one line per account+county combination
    for account_county in sorted(df["account_county"].unique()):
        series_data = df[df["account_county"] == account_county].sort_values("timestamp")
        ax.plot(
            series_data["timestamp"],
            series_data["avg_mvl_metric"],
            marker="o",
            linewidth=2,
            markersize=4,
            label=account_county,
            alpha=0.8,
        )

    ax.set_xlabel("Time", fontsize=12, fontweight="bold")
    ax.set_ylabel("Avg MVL Metric", fontsize=12, fontweight="bold")
    ax.set_title("MVL Completeness Metrics Over Time (by Account + County)", fontsize=14, fontweight="bold", pad=20)
    ax.legend(loc="best", frameon=True, shadow=True, fontsize=9)
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")

    # Save figure
    plt.tight_layout()
    plt.savefig(output_filepath, dpi=300, bbox_inches="tight", facecolor="white")
    plt.close()

    print(f"Visualization saved to {output_filepath}")


# ---------- MAIN LOGIC ----------

MAX_WORKERS = 6  # Number of parallel threads for processing accounts


def process_account(acc: dict, now: int, hours_range: int, granularity_minutes: int) -> dict:
    """
    Process a single account: fetch log group and run queries for the specified time range.

    Args:
        acc: Account dictionary with credentials
        now: Current Unix timestamp
        hours_range: How many hours to look back
        granularity_minutes: Size of each time window in minutes

    Returns a dict: {account_id: {county: [list of values for each time window]}}
    """
    account_id = acc["account_id"]
    print(f"\n=== Processing account {account_id} ===")

    # Create a session for this account
    session = boto3.Session(
        aws_access_key_id=acc["aws_access_key_id"],
        aws_secret_access_key=acc["aws_secret_access_key"],
        region_name=REGION,
    )

    # Discover log group from CloudFormation output
    log_group_name = get_log_group_name(
        session,
        REGION,
        STACK_NAME,
        LOG_GROUP_OUTPUT_KEY,
    )

    if not log_group_name:
        print(f"[WARN] Skipping account {account_id}: log group not found.")
        return {}

    print(f"Account {account_id} using log group: {log_group_name}")

    logs_client = session.client("logs", region_name=REGION)

    # Calculate number of windows
    granularity_seconds = granularity_minutes * 60
    num_windows = int((hours_range * 3600) / granularity_seconds)
    account_results = defaultdict(lambda: [None] * num_windows)

    # Query each time window
    for window_offset in range(num_windows):
        window_end = now - window_offset * granularity_seconds
        window_start = window_end - granularity_seconds

        print(
            f"  Account {account_id} | window_offset={window_offset} "
            f"| window={ts_to_iso(window_start)} .. {ts_to_iso(window_end)}"
        )

        query_results = run_insights_query(
            logs_client,
            log_group_name,
            window_start,
            window_end,
        )

        # Each `row` is a list of {field, value} dicts
        for row in query_results:
            row_dict = {col["field"]: col["value"] for col in row}
            county = row_dict.get("county")
            avg_str = row_dict.get("avg_global_completeness")

            if not county or avg_str is None:
                continue

            try:
                avg_val = float(avg_str)
            except ValueError:
                print(f"[WARN] Could not parse avg value '{avg_str}'")
                continue

            account_results[county][window_offset] = avg_val

    return {account_id: dict(account_results)}


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Collect MVL completeness metrics from AWS CloudWatch Logs across multiple accounts"
    )
    parser.add_argument("--range-hours", type=int, default=24, help="How many hours to look back (default: 24)")
    parser.add_argument(
        "--granularity-minutes", type=int, default=60, help="Size of each time window in minutes (default: 60)"
    )
    args = parser.parse_args()

    hours_range = args.range_hours
    granularity_minutes = args.granularity_minutes
    granularity_seconds = granularity_minutes * 60

    print(f"Configuration:")
    print(f"  Time range: {hours_range} hours")
    print(f"  Granularity: {granularity_minutes} minutes")
    print(f"  Number of windows: {int((hours_range * 3600) / granularity_seconds)}")

    accounts = load_accounts(ACCOUNTS_FILE)
    print(f"Loaded {len(accounts)} accounts from {ACCOUNTS_FILE}")

    now = int(time.time())  # current time in Unix seconds

    # Process accounts in parallel
    results = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_account, acc, now, hours_range, granularity_minutes): acc["account_id"]
            for acc in accounts
        }

        for future in as_completed(futures):
            account_id = futures[future]
            try:
                account_result = future.result()
                results.update(account_result)
            except Exception as e:
                print(f"[ERROR] Account {account_id} failed: {e}")

    # ---------- OUTPUT TO CSV (long format) ----------

    # Generate filename with timestamp
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    csv_filename = f"metrics_{timestamp}.csv"
    png_filename = f"metrics_{timestamp}.png"

    # Calculate number of windows
    num_windows = int((hours_range * 3600) / granularity_seconds)

    # Write CSV in long/tidy format
    with open(csv_filename, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        # Header row
        writer.writerow(["account_id", "county", "timestamp", "avg_mvl_metric"])

        for account_id, counties in sorted(results.items()):
            for county, values in sorted(counties.items()):
                # Each window gets its own row
                for window_offset in range(num_windows):
                    window_end = now - window_offset * granularity_seconds
                    timestamp_str = ts_to_iso(window_end)
                    metric_value = values[window_offset]

                    # Only write rows where we have data
                    if metric_value is not None:
                        row = [account_id, county, timestamp_str, f"{metric_value:.4f}"]
                        writer.writerow(row)

    print(f"\nResults saved to {csv_filename}")

    # ---------- CREATE VISUALIZATION ----------

    try:
        create_visualization(csv_filename, png_filename)
    except Exception as e:
        print(f"[WARN] Failed to create visualization: {e}")
        print("Ensure matplotlib, pandas, and seaborn are installed: pip install matplotlib pandas seaborn")


if __name__ == "__main__":
    # Check that accounts file exists
    if not Path(ACCOUNTS_FILE).is_file():
        raise SystemExit(f"accounts file not found: {ACCOUNTS_FILE}")
    main()
