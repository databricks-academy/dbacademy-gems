import sys, pyspark

__is_initialized = False

try:
    # noinspection PyUnresolvedReferences
    from dbacademy import dbrest
    includes_dbrest = True
except:
    includes_dbrest = False

dbgems_module = sys.modules[globals()['__name__']]

# noinspection PyGlobalUndefined
def __init_globals():
    import dbruntime

    global __is_initialized
    if __is_initialized: return
    else: __is_initialized = True

    global spark
    try: spark
    except NameError:
        spark = pyspark.sql.SparkSession.builder.getOrCreate()

    dbgems_module.spark = spark

    global sc
    try: sc
    except NameError:
        sc = spark.sparkContext

    dbgems_module.sc = sc

    global dbutils
    try:
        dbutils
    except NameError:
        if spark.conf.get("spark.databricks.service.client.enabled") == "true":
            dbutils = dbruntime.dbutils.DBUtils(spark)
        else:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]

    dbgems_module.dbutils = dbutils

def deprecation_logging_enabled():
    status = spark.conf.get("dbacademy.deprecation.printing", None)
    return status is not None and status.lower() == "enabled"

def print_warning(title: str, message: str, length: int = 80):
    title_len = length - len(title) - 3
    print(f"""* {title.upper()} {("*"*title_len)}""")
    for line in message.split("\n"):
        print(f"* {line}")
    print("*"*length)

def deprecated(reason=None):
    def decorator(inner_function):
        def wrapper(*args, **kwargs):
            if deprecation_logging_enabled():
                assert reason is not None, f"The deprecated reason must be specified."
                print_warning(title="DEPRECATED", message=reason)
            result = inner_function(*args, **kwargs)
            return result
        return wrapper
    return decorator

@deprecated(reason="Use dbgems.dbutils instead.")
def get_dbutils():  # -> dbruntime.dbutils.DBUtils:
    return dbgems_module.dbutils

@deprecated(reason="Use dbgems.spark() instead.")
def get_spark_session() -> pyspark.sql.SparkSession:
    return dbgems_module.spark

@deprecated(reason="Use dbgems.sc() instead.")
def get_session_context() -> pyspark.context.SparkContext:
    return dbgems_module.sc

def sql(query):
    return get_spark_session().sql(query)

def get_parameter(name, default_value=""):
    from py4j.protocol import Py4JJavaError
    try:
        # noinspection PyUnresolvedReferences
        result = dbutils.widgets.get(name)
        return result or default_value
    except Py4JJavaError as ex:
        if "InputWidgetNotDefined" not in ex.java_exception.getClass().getName():
            raise ex
        else:
            return default_value

def get_cloud():
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
    # noinspection PyProtectedMember,PyUnresolvedReferences
    return sc._jvm.scala.collection.JavaConversions.mapAsJavaMap(
        dbutils.entry_point.getDbutils().notebook().getContext().tags())


def get_tag(tag_name: str, default_value: str = None) -> str:
    return get_tags().get(tag_name, default_value)


def get_username() -> str:
    return get_tags()["user"]


def get_browser_host_name():
    return get_tags()["browserHostName"]


def get_job_id():
    return get_tags()["jobId"]


def is_job():
    return get_job_id() is not None


def get_workspace_id() -> str:
    # noinspection PyUnresolvedReferences
    return dbutils.entry_point.getDbutils().notebook().getContext().workspaceId().getOrElse(None)


def get_notebook_path() -> str:
    # noinspection PyUnresolvedReferences
    return dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)


def get_notebook_name() -> str:
    return get_notebook_path().split("/")[-1]


def get_notebook_dir(offset=-1) -> str:
    return "/".join(get_notebook_path().split("/")[:offset])

def get_notebooks_api_endpoint() -> str:
    # noinspection PyUnresolvedReferences
    return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)


def get_notebooks_api_token() -> str:
    # noinspection PyUnresolvedReferences
    return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

def jprint(value: dict, indent: int = 4):
    assert type(value) == dict or type(value) == list, f"Expected value to be of type \"dict\" or \"list\", found \"{type(value)}\"."

    import json
    print(json.dumps(value, indent=indent))

@deprecated(reason="Use dbacademy.dbrest.clusters.get_current_spark_version() instead.")
def get_current_spark_version(client=None):
    if includes_dbrest:
        # noinspection PyUnresolvedReferences
        from dbacademy import dbrest
        cluster_id = get_tags()["clusterId"]
        client = dbrest.DBAcademyRestClient() if client is None else client
        cluster = client.clusters().get(cluster_id)
        return cluster.get("spark_version", None)

    else:
        raise Exception(f"Cannot use rest API with-out including dbacademy.dbrest")

@deprecated(reason="Use dbacademy.dbrest.clusters.get_current_instance_pool_id() instead.")
def get_current_instance_pool_id(client=None):
    if includes_dbrest:
        # noinspection PyUnresolvedReferences
        from dbacademy import dbrest
        cluster_id = get_tags()["clusterId"]
        client = dbrest.DBAcademyRestClient() if client is None else client
        cluster = client.clusters().get(cluster_id)
        return cluster.get("instance_pool_id", None)

    else:
        raise Exception(f"Cannot use rest API with-out including dbacademy.dbrest")


@deprecated(reason="Use dbacademy.dbrest.clusters.get_current_node_type_id() instead.")
def get_current_node_type_id(client=None):

    if includes_dbrest:
        # noinspection PyUnresolvedReferences
        from dbacademy import dbrest
        cluster_id = get_tags()["clusterId"]
        client = dbrest.DBAcademyRestClient() if client is None else client
        cluster = client.clusters().get(cluster_id)
        return cluster.get("node_type_id", None)

    else:
        raise Exception(f"Cannot use rest API with-out including dbacademy.dbrest")


# noinspection PyUnresolvedReferences
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

    import dbruntime
    from py4j.java_collections import JavaMap

    value = dbgems_module.dbutils
    assert isinstance(value, dbruntime.dbutils.DBUtils), f"Expected {dbruntime.dbutils.DBUtils}, found {type(value)}"

    value = dbgems_module.spark
    assert isinstance(value, pyspark.sql.SparkSession), f"Expected {pyspark.sql.SparkSession}, found {type(value)}"

    value = dbgems_module.sc
    assert isinstance(value, pyspark.context.SparkContext), f"Expected {pyspark.context.SparkContext}, found {type(value)}"

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

    if not includes_dbrest:
        print_deprecation_warning(title="DEPENDENCY ERROR", message="The methods get_current_spark_version(), get_current_instance_pool_id() and get_current_node_type_id() require inclusion of the dbacademy_rest libraries")
    else:
        value = get_current_spark_version()
        assert value == expected_get_current_spark_version, f"Expected \"{expected_get_current_spark_version}\", found \"{value}\"."

        value = get_current_instance_pool_id()
        assert value == expected_get_current_instance_pool_id, f"Expected \"{expected_get_current_instance_pool_id}\", found \"{value}\"."

        value = get_current_node_type_id()
        assert value == expected_get_current_node_type_id, f"Expected \"{expected_get_current_node_type_id}\", found \"{value}\"."

    print("All tests passed!")

def display_html(html) -> None:
    import inspect
    caller_frame = inspect.currentframe().f_back
    while caller_frame is not None:
        caller_globals = caller_frame.f_globals
        function = caller_globals.get("displayHTML")
        if function:
            return function(html)
        caller_frame = caller_frame.f_back
    raise ValueError("displayHTML not found in any caller frames.")

def display(html) -> None:
    import inspect
    caller_frame = inspect.currentframe().f_back
    while caller_frame is not None:
        caller_globals = caller_frame.f_globals
        function = caller_globals.get("display")
        if function:
            return function(html)
        caller_frame = caller_frame.f_back
    raise ValueError("display not found in any caller frames.")


__init_globals()

sc = dbgems_module.sc
spark = dbgems_module.spark
dbutils = dbgems_module.dbutils
