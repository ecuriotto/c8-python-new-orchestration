# Camunda 8 Python Job Worker

A Python job worker application using the [Camunda Orchestration Cluster API Python SDK](https://docs.camunda.io/docs/next/apis-tools/python-sdk/) that handles two service tasks from the Payment process:

- **`credit-deduction`** — Deduct credit
- **`charg-credit-card`** — Charge credit card

## Requirements

- Python 3.10 or later

## Setup

### 1. Create and activate a virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Set connection environment variables

Export your Camunda cluster connection properties before running the worker:

```bash
export CAMUNDA_CLIENT_ID='<your-client-id>'
export CAMUNDA_CLIENT_SECRET='<your-client-secret>'
export CAMUNDA_CLUSTER_ID='<your-cluster-id>'
export CAMUNDA_CLUSTER_REGION='<your-region>'
```

### 4. Run the worker

```bash
python worker.py
```

The worker will start polling for `credit-deduction` and `charg-credit-card` jobs and log a message each time a job is handled.
