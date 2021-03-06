__is_initialized=False

def __init():
    global SparkSession
    from pyspark.sql import SparkSession
    global __is_initialized
    if __is_initialized:
      return
    __is_initialized=True
    global spark
    global sc
    global dbutils

    try:
        spark
    except NameError:
        spark = SparkSession.builder.getOrCreate()

    try:
        sc
    except NameError:
        sc = spark.sparkContext

    try:
        dbutils
    except NameError:
        if spark.conf.get("spark.databricks.service.client.enabled") == "true":
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        else:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]


def get_parameter(name, default_value=""):
    __init()
    try: return str(dbutils.widgets.get(name))
    except: return default_value


def get_current_spark_version(client=None):
    __init()
    print("*" * 80)
    print("* DEPRECATION WARNING")
    print("* dbacademy.dbrest.clusters().get_current_spark_version() instead")
    print("*" * 80)

    from dbacademy import dbrest
    cluster_id = get_tags()["clusterId"]
    client = dbrest.DBAcademyRestClient() if client is None else client
    cluster = client.clusters().get(cluster_id)
    return cluster.get("spark_version", None)


def get_current_instance_pool_id(client=None):
    __init()
    print("*" * 80)
    print("* DEPRECATION WARNING")
    print("* dbacademy.dbrest.clusters().get_current_instance_pool_id() instead")
    print("*" * 80)

    cluster_id = get_tags()["clusterId"]
    client = dbrest.DBAcademyRestClient() if client is None else client
    cluster = client.clusters().get(cluster_id)
    return cluster.get("instance_pool_id", None)


def get_current_node_type_id(client=None):
    __init()
    print("*" * 80)
    print("* DEPRECATION WARNING")
    print("* dbacademy.dbrest.clusters().get_current_node_type_id() instead")
    print("*" * 80)

    cluster_id = get_tags()["clusterId"]
    client = dbrest.DBAcademyRestClient() if client is None else client
    cluster = client.clusters().get(cluster_id)
    return cluster.get("node_type_id", None)


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
