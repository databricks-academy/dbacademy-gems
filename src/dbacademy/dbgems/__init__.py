import dbacademy_gems.dbgems

from dbacademy_gems.dbgems import print_warning, deprecation_logging_enabled

if deprecation_logging_enabled():
    print_warning(title="DEPRECATED", message=f"dbacademy.dbgems is deprecated, use dbacademy_gems.dbgems instead.")


get_browser_host_name = dbacademy_gems.dbgems.get_browser_host_name
get_cloud = dbacademy_gems.dbgems.get_cloud
get_current_instance_pool_id = dbacademy_gems.dbgems.get_current_instance_pool_id
get_current_node_type_id = dbacademy_gems.dbgems.get_current_node_type_id
get_current_spark_version = dbacademy_gems.dbgems.get_current_spark_version

dbutils = dbacademy_gems.dbgems.dbutils
get_dbutils = dbacademy_gems.dbgems.get_dbutils

get_job_id = dbacademy_gems.dbgems.get_job_id
get_notebook_dir = dbacademy_gems.dbgems.get_notebook_dir
get_notebook_name = dbacademy_gems.dbgems.get_notebook_name
get_notebook_path = dbacademy_gems.dbgems.get_notebook_path
get_notebooks_api_endpoint = dbacademy_gems.dbgems.get_notebooks_api_endpoint
get_notebooks_api_token = dbacademy_gems.dbgems.get_notebooks_api_token
get_parameter = dbacademy_gems.dbgems.get_parameter

sc = dbacademy_gems.dbgems.sc
get_session_context = dbacademy_gems.dbgems.get_session_context

spark = dbacademy_gems.dbgems.spark
get_spark_session = dbacademy_gems.dbgems.get_spark_session

get_tag = dbacademy_gems.dbgems.get_tag
get_tags = dbacademy_gems.dbgems.get_tags
get_username = dbacademy_gems.dbgems.get_username
get_workspace_id = dbacademy_gems.dbgems.get_workspace_id
is_job = dbacademy_gems.dbgems.is_job
proof_of_life = dbacademy_gems.dbgems.proof_of_life
