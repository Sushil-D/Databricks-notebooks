# Databricks notebook source
# MAGIC %md
# MAGIC ## Overview
# MAGIC This is sample notebook to create job and add permssion as **CAN_MANAGE** to the created job. you can change the permssion as required.
# MAGIC 
# MAGIC This notebook is written in **Python** so the default cell type is Python.

# COMMAND ----------

# DBTITLE 1,Job Json
job_json = {
  "name": "sample-job-with-appid",
  "new_cluster": {
      "custom_tags": {
          "component": "component- tag"
      },
      "spark_version": "7.3.x-scala2.12",
      "aws_attributes": {
          "zone_id": "us-east-1a",
          "first_on_demand": 1,
          "availability": "SPOT_WITH_FALLBACK",
          "instance_profile_arn": "arn:aws:iam::[account-id]:instance-profile/my-profile",
          "spot_bid_price_percent": 102,
          "ebs_volume_count": 0
      },
      "node_type_id": "i3.xlarge",
      "spark_env_vars": {
          "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
      },
      "enable_elastic_disk": True,
      "autoscale": {
          "min_workers": 1,
          "max_workers": 2
      },
      "num_workers": 2
  },
  "libraries": [
      {
          "egg": "s3://my-bucket/libraries/edl/latest.egg"
      },
      {
          "egg": "dbfs:/mnt/databricks/Sessionize.egg"
      }
  ],
  "email_notifications": {
      "on_start": [
          "user.name@mydomain.com"
      ],
      "on_success": [
          "user.name@mydomain.com"
      ],
      "on_failure": [
          "user.name@mydomain.com"
      ]
  },
  "timeout_seconds": 3600,
  "notebook_task": {
      "notebook_path": "/Users/user.name@mydomain.com/default-job-notebook",
      "revision_timestamp": 0,
      "base_parameters": {
          "ENVIRONMENT": "devl"
      }
  },
  "max_concurrent_runs": 1
}


# COMMAND ----------

# DBTITLE 1,functions
import requests
import json
import re
import pprint

def init():
  """Initial function of the notebook.
  Executes everytime a widget is changed or this notebook is refreshed.
  """
  
  env =['None','https://mydomain.cloud.databricks.com',]
  dbutils.widgets.text("1 User Group or User email Id",'')
  dbutils.widgets.dropdown("2 Databricks Env", 'None', [str(x) for x in env])
  dbutils.widgets.dropdown("3 create Job", 'No', ['Yes','No'])

  
def display_error(err_msg):
  """Display HTML Error message in the cell."""
  displayHTML("""<h4 style="display: inline-block;"><g-emoji class="g-emoji" alias="stop_sign" fallback-src="https://github.com/images/icons/emoji/unicode/1f6d1.png">🛑</g-emoji>  Error: {} </h4>""".format(err_msg))


def display_info(msg):
  """Display HTML Info message in the cell."""
  displayHTML("""<h4 style="display: inline-block;"><g-emoji class="g-emoji" alias="info" fallback-src="https:///github.com/images/icons/emoji/unicode/2139.png">🛈</g-emoji>  Info : {} </h4> """.format(msg))


def display_warning(msg):
  """Display HTML Warning message in the cell."""
  displayHTML("""<h4 style="display: inline-block;"><g-emoji class="g-emoji" alias="warning" fallback-src="https://github.com/images/icons/emoji/unicode/26a0.png">⚠️</g-emoji> Warning : {} </h4> """.format(msg))


def display_success(msg):
  """Display HTML Success message in the cell."""
  displayHTML("""<h4 style="display: inline-block;"><g-emoji class="g-emoji" alias="heavy_check_mark" fallback-src="https://github.com/images/icons/emoji/unicode/2714.png">✅</g-emoji> Success : {} </h4> """.format(msg))  

def isValid(email):
    regex = re.compile(r'([A-Za-z0-9]+[.-_])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\.[A-Z|a-z]{2,})+')
    
    if re.fullmatch(regex, email):
      return True
    else:
      return False


def get_token (secrets_scope_name, secrets_scope_key):
	db_token = dbutils.secrets.get(secrets_scope_name, secrets_scope_key)
	return db_token
    

def create_header(db_token):
  return({
          'Content-Type': 'application/json',
          'Authorization': 'Bearer {0}'.format(db_token)
      })

def create_job( db_job_url, db_token, payload):
  db_headers = create_header(db_token)
  job_id = 0
  response = requests.post(f'{db_job_url}/api/2.0/jobs/create', headers=db_headers, data=json.dumps(payload))
  if response.status_code == 200 :
    resp = json.loads(response.text)
    job_id = resp["job_id"]
  else:
    raise NameError(json.loads(response.text))
  return (job_id)


def get_request( db_api_url, db_token):
  db_headers = create_header(db_token)
  response = requests.get(db_api_url, headers=db_headers)
  extract = json.loads(response.text)
  return (extract)

def put_permission( db_job_url, db_token, job_id, usr_grp):
# sample group name and appid
# group_name = "my-user-group"
# user_name = "user.name@mydomain.com"
# user_name = "appid@mydomain.com"
  if isValid(usr_grp) :
    new_access= {
      "access_control_list": [
        {
          "user_name": usr_grp,
          "permission_level": "CAN_MANAGE"
        }
      ]
    }
  else :
    new_access= {
      "access_control_list": [
        {
          "group_name": usr_grp ,
          "permission_level": "CAN_MANAGE"
        }
      ]
    }
  
  json_new_access = json.dumps(new_access, indent = 4) 
  db_headers  = create_header(db_token)
  rqust = requests.patch(f'{db_job_url}/api/2.0/preview/permissions/jobs/{job_id}', headers=db_headers, data = json_new_access)
  print("Updated Permssion response : ")
  pprint.pprint(json.loads(rqust.text))
  if rqust.status_code == 200 :
    return True
  else :
    return False


# COMMAND ----------

# DBTITLE 1,main function call
usr_grp =  "my-user-group"

if __name__ == "__main__":
  try:
    init()
    db_job_url = dbutils.widgets.get("2 Databricks Env")
    usr_grp  = dbutils.widgets.get("1 User Group or User email Id")
    
    if len(usr_grp.strip()) == 0 :
        display_info("Please enter User group or email")
    else:
      if db_job_url == "None":
        # if Databricks env is not selected
        display_info("Select a databricks Environement")
      else:
        # if Databricks env is selected
          job_creation = dbutils.widgets.get("3 create Job")
          if job_creation == "No":
            # if create job action is NO
            display_info("Select Yes to create job")
          else:
            #create job
            job_id = create_job( db_job_url, get_token(db_job_url), job_json)
            display_info(f"job Created : {job_id}")
            rslt = put_permission( db_job_url, get_token(db_job_url), job_id, usr_grp)

  except Exception as ex:
    display_error(ex)
    init()
