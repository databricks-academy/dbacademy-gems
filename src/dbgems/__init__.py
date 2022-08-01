from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from pyspark.context import SparkContext

__is_initialized = False


# noinspection PyGlobalUndefined
def __init():
    global SparkSession

    global __is_initialized
    if __is_initialized: return
    else: __is_initialized = True

    global spark
    try: spark
    except NameError:
        spark = SparkSession.builder.getOrCreate()

    global sc
    try: sc
    except NameError:
        sc = spark.sparkContext

    global dbutils
    try: dbutils
    except NameError:
        if spark.conf.get("spark.databricks.service.client.enabled") == "true":
            dbutils = DBUtils(spark)
        else:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]


def get_dbutils() -> DBUtils:
    __init()
    return dbutils


def get_spark_session() -> SparkSession:
    __init()
    return spark


def get_session_context() -> SparkContext:
    __init()
    return sc


def get_parameter(name, default_value=""):
    __init()
    try: return str(dbutils.widgets.get(name))
    except: return default_value


def get_cloud():
    __init()
    with open("/databricks/common/conf/deploy.conf") as f:
        for line in f:
            if "databricks.instance.metadata.cloudProvider" in line and "\"GCP\"" in line:
                return "GCP"
            elif "databricks.instance.metadata.cloudProvider" in line and "\"AWS\"" in line:
                return "AWS"
            elif "databricks.instance.metadata.cloudProvider" in line and "\"Azure\"" in line:
                return "MSA"

    raise Exception("Unable to identify the cloud provider.")


def get_tags() -> dict:
    __init()
    # noinspection PyProtectedMember
    return sc._jvm.scala.collection.JavaConversions.mapAsJavaMap(
        dbutils.entry_point.getDbutils().notebook().getContext().tags())


def get_tag(tag_name: str, default_value: str = None) -> str:
    __init()
    return get_tags().get(tag_name, default_value)


def get_username() -> str:
    __init()
    return get_tags()["user"]


def get_browser_host_name():
    __init()
    return get_tags()["browserHostName"]


def get_job_id():
    __init()
    return get_tags()["jobId"]


def is_job():
    __init()
    return get_job_id() is not None


def get_workspace_id() -> str:
    __init()
    return dbutils.entry_point.getDbutils().notebook().getContext().workspaceId().getOrElse(None)


def get_notebook_path() -> str:
    __init()
    return dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)


def get_notebook_name() -> str:
    __init()
    return get_notebook_path().split("/")[-1]


def get_notebook_dir(offset=-1) -> str:
    __init()
    return "/".join(get_notebook_path().split("/")[:offset])


def get_notebooks_api_endpoint() -> str:
    __init()
    return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)


def get_notebooks_api_token() -> str:
    __init()
    return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)


def get_current_spark_version(client=None):
    print("*" * 80)
    print("* DEPRECATION WARNING")
    print("* dbacademy.dbrest.clusters().get_current_spark_version() instead")
    print("*" * 80)
    __init()

    from dbacademy import dbrest
    cluster_id = get_tags()["clusterId"]
    client = dbrest.DBAcademyRestClient() if client is None else client
    cluster = client.clusters().get(cluster_id)
    return cluster.get("spark_version", None)


def get_current_instance_pool_id(client=None):
    print("*" * 80)
    print("* DEPRECATION WARNING")
    print("* dbacademy.dbrest.clusters().get_current_instance_pool_id() instead")
    print("*" * 80)
    __init()

    from dbacademy import dbrest
    cluster_id = get_tags()["clusterId"]
    client = dbrest.DBAcademyRestClient() if client is None else client
    cluster = client.clusters().get(cluster_id)
    return cluster.get("instance_pool_id", None)


def get_current_node_type_id(client=None):
    print("*" * 80)
    print("* DEPRECATION WARNING")
    print("* dbacademy.dbrest.clusters().get_current_node_type_id() instead")
    print("*" * 80)
    __init()

    from dbacademy import dbrest
    cluster_id = get_tags()["clusterId"]
    client = dbrest.DBAcademyRestClient() if client is None else client
    cluster = client.clusters().get(cluster_id)
    return cluster.get("node_type_id", None)


def proof_of_life(expected_get_username,
                  expected_get_tag,
                  expected_get_browser_host_name,
                  expected_get_workspace_id,
                  expected_get_notebook_path,
                  expected_get_notebook_name,
                  expected_get_notebook_dir,
                  expected_get_notebooks_api_endpoint,
                  expected_get_current_spark_version,
                  expected_get_current_instance_pool_id,
                  expected_get_current_node_type_id):
    """Because it is too difficult to validate this from the command line, this functio simply invokes all the functions as proof of life"""

    from py4j.java_collections import JavaMap

    value = get_dbutils()
    assert type(value) == DBUtils, f"Expected {type(DBUtils)}, found {type(value)}"

    value = get_spark_session()
    assert type(value) == SparkSession, f"Expected {type(SparkSession)}, found {type(value)}"

    value = get_session_context()
    assert type(value) == SparkContext, f"Expected {type(SparkContext)}, found {type(value)}"

    value = get_parameter("some_widget", default_value="undefined")
    assert value == "undefined", f"Expected \"undefined\", found \"{value}\"."

    value = get_cloud()
    assert value == "AWS", f"Expected \"AWS\", found \"{value}\"."

    value = get_tags()
    assert type(value) == JavaMap, f"Expected type \"dict\", found \"{type(value)}\"."

    value = get_tag("orgId")
    assert value == expected_get_tag, f"Expected \"{expected_get_tag}\", found \"{value}\"."

    value = get_username()
    assert value == expected_get_username, f"Expected \"{expected_get_username}\", found \"{value}\"."

    value = get_browser_host_name()
    assert value == expected_get_browser_host_name, f"Expected \"{expected_get_browser_host_name}\", found \"{value}\"."

    value = get_job_id()
    assert value is None, f"Expected \"None\", found \"{value}\"."

    value = is_job()
    assert value is False, f"Expected \"{False}\", found \"{value}\"."

    value = get_workspace_id()
    assert value == expected_get_workspace_id, f"Expected \"{expected_get_workspace_id}\", found \"{value}\"."

    value = get_notebook_path()
    assert value == expected_get_notebook_path, f"Expected \"{expected_get_notebook_path}\", found \"{value}\"."

    value = get_notebook_name()
    assert value == expected_get_notebook_name, f"Expected \"{expected_get_notebook_name}\", found \"{value}\"."

    value = get_notebook_dir()
    assert value == expected_get_notebook_dir, f"Expected \"{expected_get_notebook_dir}\", found \"{value}\"."

    value = get_notebooks_api_endpoint()
    assert value == expected_get_notebooks_api_endpoint, f"Expected \"{expected_get_notebooks_api_endpoint}\", found \"{value}\"."

    value = get_notebooks_api_token()
    assert value is not None, f"Expected not-None."

    value = get_current_spark_version()
    assert value == expected_get_current_spark_version, f"Expected \"{expected_get_current_spark_version}\", found \"{value}\"."

    value = get_current_instance_pool_id()
    assert value == expected_get_current_instance_pool_id, f"Expected \"{expected_get_current_instance_pool_id}\", found \"{value}\"."

    value = get_current_node_type_id()
    assert value == expected_get_current_node_type_id, f"Expected \"{expected_get_current_node_type_id}\", found \"{value}\"."

    print("All tests passed!")
