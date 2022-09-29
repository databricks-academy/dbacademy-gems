from dbacademy_gems import dbgems as other_dbgems

if other_dbgems.deprecation_logging_enabled():
    other_dbgems.print_warning(title="DEPRECATED", message=f"dbacademy.dbgems is deprecated, use dbacademy_gems.dbgems instead.")

get_browser_host_name = other_dbgems.get_browser_host_name
get_cloud = other_dbgems.get_cloud
get_current_instance_pool_id = other_dbgems.get_current_instance_pool_id
get_current_node_type_id = other_dbgems.get_current_node_type_id
get_current_spark_version = other_dbgems.get_current_spark_version
get_dbutils = other_dbgems.get_dbutils
get_job_id = other_dbgems.get_job_id
get_notebook_dir = other_dbgems.get_notebook_dir
get_notebook_name = other_dbgems.get_notebook_name
get_notebook_path = other_dbgems.get_notebook_path
get_notebooks_api_endpoint = other_dbgems.get_notebooks_api_endpoint
get_notebooks_api_token = other_dbgems.get_notebooks_api_token
get_parameter = other_dbgems.get_parameter
get_session_context = other_dbgems.get_session_context
get_spark_session = other_dbgems.get_spark_session
get_tag = other_dbgems.get_tag
get_tags = other_dbgems.get_tags
get_username = other_dbgems.get_username
get_workspace_id = other_dbgems.get_workspace_id
is_job = other_dbgems.is_job
proof_of_life = other_dbgems.proof_of_life
