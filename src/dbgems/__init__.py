from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from pyspark.context import SparkContext

__is_initialized = False


def deprecated(func):
    """This is a decorator which can be used to mark functions
    as deprecated. It will result in a warning being emitted
    when the function is used."""
    import warnings
    import functools

    @functools.wraps(func)
    def new_func(*args, **kwargs):
        warnings.simplefilter('always', DeprecationWarning)  # turn off filter
        warnings.warn("Call to deprecated function {}.".format(func.__name__),
                      category=DeprecationWarning,
                      stacklevel=2)
        warnings.simplefilter('default', DeprecationWarning)  # reset filter
        return func(*args, **kwargs)
    return new_func


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
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        else:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]


def get_dbutils() -> dbutils:
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


@deprecated
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


@deprecated
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


@deprecated
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


def proof_of_life():
    """Because it is too difficult to validate this from the command line, this functio simply invokes all the functions as proof of life"""
    get_dbutils()
    get_spark_session()
    get_session_context()
    get_parameter(name, default_value="")
    get_cloud()
    get_tags()
    get_tag(tag_name: str, default_value: str = None)
    get_username()
    get_browser_host_name()
    get_job_id()
    is_job()
    get_workspace_id()
    get_notebook_path()
    get_notebook_name()
    get_notebook_dir(offset=-1)
    get_notebooks_api_endpoint()
    get_notebooks_api_token()
    get_current_spark_version(client=None)
    get_current_instance_pool_id(client=None)
    get_current_node_type_id(client=None)
    print("All tests passed!")
