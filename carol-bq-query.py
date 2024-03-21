from pycarol import Carol, PwdAuth
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import json, os

load_dotenv(".env")

CAROLUSER = os.environ.get("CAROLUSER")
CAROLPWD = os.environ.get("CAROLPWD")
CAROLTENANT = os.environ.get("CAROLTENANT")
CAROLORGANIZATION = os.environ.get("CAROLORGANIZATION")
CAROLCONNECTORID= os.environ.get("CAROLCONNECTORID")
CAROLSA = json.loads(os.environ.get("CAROLSA"))

sql_query = '''
select * from shd_Protheus_Workstations limit 10
'''

carol = Carol(auth=PwdAuth(CAROLUSER, CAROLPWD), organization=CAROLORGANIZATION, domain=CAROLTENANT, connector_id=CAROLCONNECTORID)

credentials = service_account.Credentials.from_service_account_info(CAROLSA)
client = bigquery.Client(credentials=credentials)
config = bigquery.QueryJobConfig(priority="BATCH", default_dataset=f"{CAROLSA['project_id']}.{carol._current_env()['mdmId']}")
try:
        query_job = client.query(sql_query, job_config=config, )
        df = query_job.result().to_dataframe(create_bqstorage_client=True)
        df.to_csv("result.csv", index=False)
except Exception as e:
        print("error: {}".format(e))
