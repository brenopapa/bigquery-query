from pycarol import Carol, PwdAuth, BQ
# from google.cloud import bigquery
# from google.oauth2 import service_account
from dotenv import load_dotenv
import json, os

load_dotenv(".env")

CAROLUSER = os.environ.get("CAROLUSER")
CAROLPWD = os.environ.get("CAROLPWD")
CAROLTENANT = os.environ.get("CAROLTENANT")
CAROLAPP = os.environ.get("CAROLAPP")
CAROLORGANIZATION = os.environ.get("CAROLORGANIZATION")
CAROLCONNECTORID= os.environ.get("CAROLCONNECTORID")
CAROLSA = json.loads(os.environ.get("CAROLSA"))

sql_query = '''
select * from shd_Protheus_workstations limit 10
'''

carol = Carol(auth=PwdAuth(CAROLUSER, CAROLPWD), organization=CAROLORGANIZATION, domain=CAROLTENANT, connector_id=CAROLCONNECTORID, app_name=CAROLAPP)

try:
        result = BQ(carol).query(sql_query)
        result.to_csv("result.csv", index=False)
except Exception as e:
        print("error: {}".format(e))
