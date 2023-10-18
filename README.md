# bigquery-query

Use .env file with:
```bash
CAROLUSER = your carol user
CAROLPWD = your carol pwd
CAROLTENANT = target tenant
CAROLORGANIZATION = tenant organization
CAROLCONNECTORID = tenant connectorid
CAROLSA = carol serviceaccount json
```

To use the script:
```bash
python -m venv .venv
.\.venv\Scripts\activate on Windows or source .venv/bin/activate on Mac
python -m pip install -r requirements.txt
python carol-bq-query.py
```

