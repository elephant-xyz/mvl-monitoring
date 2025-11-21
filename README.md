# MVL Monitoring - CloudWatch Metrics Collection

This tool collects and visualizes MVL (Mirror Validator Logger) completeness metrics from multiple AWS accounts using CloudWatch Logs Insights.

## Overview

The `collect_metrics.py` script:
- Queries CloudWatch Logs across multiple AWS accounts in parallel
- Collects MVL completeness metrics for the last 24 hours (hourly windows)
- Exports data to CSV format for further analysis
- Generates a time series visualization showing metrics by account and county

## Prerequisites

Install required Python dependencies:

```bash
pip install -r requirements.txt
```

Or manually:

```bash
pip install boto3 pyyaml matplotlib pandas seaborn
```

## Configuration

### 1. Create `accounts.yaml`

Create a file named `accounts.yaml` in the same directory as the script with the following structure:

```yaml
- Account ID: 123456789012
  username: user1@example.com
  password: optional_field
  aws_access_key_id: AKIAIOSFODNN7EXAMPLE
  aws_secret_access_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

- Account ID: 987654321098
  username: user2@example.com
  password: optional_field
  aws_access_key_id: AKIAI44QH8DHBEXAMPLE
  aws_secret_access_key: je7MtGbClwBF/2Zp9Utk/h3yCo8nvbEXAMPLEKEY
```

**Required fields:**
- `Account ID`: AWS account number (will be used as identifier)
- `aws_access_key_id`: AWS access key with CloudWatch Logs and CloudFormation read permissions
- `aws_secret_access_key`: Corresponding AWS secret key

### 2. AWS Permissions

The AWS credentials must have permissions for:
- `cloudformation:DescribeStacks` - to fetch log group names from CloudFormation outputs
- `logs:StartQuery` - to initiate CloudWatch Logs Insights queries
- `logs:GetQueryResults` - to retrieve query results

### 3. Script Configuration

Adjust these constants in `collect_metrics.py` if needed:

```python
ACCOUNTS_FILE = "accounts.yaml"          # Path to accounts file
REGION = "us-east-1"                     # AWS region
STACK_NAME = "elephant-oracle-node"      # CloudFormation stack name
LOG_GROUP_OUTPUT_KEY = "WorkflowMirrorValidatorLogGroupName"  # CF output key
MAX_WORKERS = 6                          # Number of parallel threads
```

## Usage

Run the script:

```bash
python collect_metrics.py
```

### Output Files

The script generates two files with timestamps:

1. **CSV file** (`metrics_YYYYMMDD_HHMMSS.csv`):
   - Long format dataset with columns: `account_id`, `county`, `timestamp`, `avg_mvl_metric`
   - One row per measurement (account + county + hour)
   - Suitable for further analysis with pandas/Excel

2. **PNG visualization** (`metrics_YYYYMMDD_HHMMSS.png`):
   - Time series plot showing MVL metrics over 24 hours
   - One line per (Account + County) combination
   - High resolution (300 DPI) for reports

## How It Works

1. **Load accounts** from `accounts.yaml`
2. **Parallel processing**: Each account is processed in a separate thread
3. **For each account**:
   - Look up the CloudFormation stack to get the log group name
   - Query CloudWatch Logs Insights for 24 one-hour windows
   - Extract MVL completeness metrics grouped by county
4. **Export results** to CSV and generate visualization

## Query Details

The script runs this CloudWatch Logs Insights query for each hour:

```sql
fields
    message.county as county,
    message.global_completeness as completeness,
    message.msg as msg
| filter msg = "mvl_completeness_metric"
| stats avg(completeness) as avg_global_completeness by county
```

## Troubleshooting

- **"accounts file not found"**: Create `accounts.yaml` in the script directory
- **"Failed to describe stack"**: Check AWS credentials and CloudFormation stack name
- **"Failed to start query"**: Verify CloudWatch Logs permissions
- **Missing visualization**: Install matplotlib, pandas, and seaborn

## Example Output

```
Loaded 2 accounts from accounts.yaml

=== Processing account 123456789012 ===
Account 123456789012 using log group: /aws/lambda/mvl-validator
  Account 123456789012 | hour_offset=0 | window=2025-11-21T16:00:00Z .. 2025-11-21T17:00:00Z
  ...

Results saved to metrics_20251121_174841.csv
Visualization saved to metrics_20251121_174841.png
```
