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
select estabelecimento.cod_id_feder as Empresa,
       sdo_ctbl.cod_cta_ctbl as Conta,
       cta_ctbl.des_tit_ctbl as Descricao_Conta,
       '' as Conta_Referencia,
       '' as Descricao_Conta_Referencia,
       cta_ctbl.ind_natur_cta_ctbl as Condicao_Normal,
       'Analítica' as Classe_Conta,
       '' as Natureza_Conta,
       sdo_ctbl.val_sdo_ctbl_fim + sdo_ctbl.val_sdo_ctbl_db - sdo_ctbl.val_sdo_ctbl_cr as Saldo_Anterior,
       sdo_ctbl.val_sdo_ctbl_db as Debito,
       sdo_ctbl.val_sdo_ctbl_cr as Credito,
       sdo_ctbl.val_sdo_ctbl_fim as Saldo_Atual,
       sdo_ctbl.dat_sdo_ctbl as Competencia,
       '' as Status
       from deduplicated_stg_datasul_carol_movfin_sdo_ctbl as sdo_ctbl
       left join deduplicated_stg_datasul_carol_emsuni_estabelecimento estabelecimento 
        on estabelecimento.cod_empresa = sdo_ctbl.cod_empresa
       and estabelecimento.log_estab_princ = true
       left join deduplicated_stg_datasul_carol_emsuni_cta_ctbl cta_ctbl
        on cta_ctbl.cod_plano_cta_ctbl = sdo_ctbl.cod_plano_cta_ctbl
       and cta_ctbl.cod_cta_ctbl = sdo_ctbl.cod_cta_ctbl
       left join deduplicated_stg_datasul_carol_emsuni_estrut_cta_ctbl estrut_cta_ctbl
        on estrut_cta_ctbl.cod_plano_cta_ctbl = sdo_ctbl.cod_plano_cta_ctbl
       and estrut_cta_ctbl.cod_cta_ctbl_pai = sdo_ctbl.cod_cta_ctbl
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
