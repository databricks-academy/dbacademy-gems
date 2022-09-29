import pyspark
from typing import Union

def find_global(target):
    import inspect
    global dbgems
    caller_frame = inspect.currentframe().f_back

    while caller_frame is not None:
        caller_globals = caller_frame.f_globals
        what = caller_globals.get(target)
        if what:
            return what
        caller_frame = caller_frame.f_back

    dbgems.print_warning(title="DEPENDENCY ERROR", message=f"Global attribute {target} not found in any caller frames.")
    return None


_sc = find_global("sc")
_spark = find_global("spark")
_dbutils = find_global("dbutils")


def deprecated(reason=None):
    def decorator(inner_function):
        def wrapper(*args, **kwargs):
            global dbgems
            if dbgems.deprecation_logging_enabled():
                assert reason is not None, f"The deprecated reason must be specified."
                try:
                    import inspect
                    function_name = str(inner_function.__name__) + str(inspect.signature(inner_function))
                    final_reason = f"From: {reason}\n{function_name}"
                except:
                    final_reason = reason  # just in case

                DBGems.print_warning(title="DEPRECATED", message=final_reason)

            return inner_function(*args, **kwargs)

        return wrapper

    return decorator

class MockDBUtils:
    def __init__(self):
        self.fs = None
        self.widgets = None
        self.notebook = None

    # noinspection PyPep8Naming
    def displayHTML(self, **kwargs): pass

    # noinspection PyPep8Naming
    def display(self, **kwargs): pass

class DBGems:
    def __init__(self):
        try:
            # noinspection PyUnresolvedReferences
            from dbacademy import dbrest
            self.includes_dbrest = True
        except:
            self.includes_dbrest = False

    @property
    def spark(self) -> Union[None, pyspark.sql.SparkSession]:
        global _spark
        return _spark

    @deprecated(reason="Use dbgems.spark instead.")
    def get_spark_session(self) -> pyspark.sql.SparkSession:
        return self.spark

    @property
    def sc(self) -> Union[None, pyspark.SparkContext]:
        global _sc
        return _sc

    @deprecated(reason="Use dbgems.sc instead.")
    def get_session_context(self) -> pyspark.context.SparkContext:
        return self.sc

    @property
    def dbutils(self) -> Union[None, MockDBUtils]:
        global _dbutils
        return _dbutils

    @deprecated(reason="Use dbgems.dbutils instead.")
    def get_dbutils(self) -> Union[None, MockDBUtils]:
        return self.dbutils

    def deprecation_logging_enabled(self):
        status = self.spark.conf.get("dbacademy.deprecation.logging", None)
        return status is not None and status.lower() == "enabled"

    @staticmethod
    def print_warning(title: str, message: str, length: int = 80):
        title_len = length - len(title) - 3
        print(f"""* {title.upper()} {("*"*title_len)}""")
        for line in message.split("\n"):
            print(f"* {line}")
        print("*"*length)

    def sql(self, query):
        return self.spark.sql(query)

    def get_parameter(self, name, default_value=""):
        from py4j.protocol import Py4JJavaError
        try:
            result = self.dbutils.widgets.get(name)
            return result or default_value
        except Py4JJavaError as ex:
            if "InputWidgetNotDefined" not in ex.java_exception.getClass().getName():
                raise ex
            else:
                return default_value

    @staticmethod
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

    def get_tags(self) -> dict:
        # noinspection PyUnresolvedReferences
        java_tags = self.dbutils.entry_point.getDbutils().notebook().getContext().tags()

        # noinspection PyProtectedMember
        return self.sc._jvm.scala.collection.JavaConversions.mapAsJavaMap(java_tags)

    def get_tag(self, tag_name: str, default_value: str = None) -> str:
        return self.get_tags().get(tag_name, default_value)

    def get_username(self) -> str:
        return self.get_tags()["user"]

    def get_browser_host_name(self):
        return self.get_tags()["browserHostName"]

    def get_job_id(self):
        return self.get_tags()["jobId"]

    def is_job(self):
        return self.get_job_id() is not None

    def get_workspace_id(self) -> str:
        # noinspection PyUnresolvedReferences
        return self.dbutils.entry_point.getDbutils().notebook().getContext().workspaceId().getOrElse(None)

    def get_notebook_path(self) -> str:
        # noinspection PyUnresolvedReferences
        return self.dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)

    def get_notebook_name(self) -> str:
        return self.get_notebook_path().split("/")[-1]

    def get_notebook_dir(self, offset=-1) -> str:
        return "/".join(self.get_notebook_path().split("/")[:offset])

    def get_notebooks_api_endpoint(self) -> str:
        return self.dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)

    def get_notebooks_api_token(self) -> str:
        return self.dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

    @staticmethod
    def jprint(value: dict, indent: int = 4):
        assert type(value) == dict or type(value) == list, f"Expected value to be of type \"dict\" or \"list\", found \"{type(value)}\"."

        import json
        print(json.dumps(value, indent=indent))

    @deprecated(reason="Use dbacademy.dbrest.clusters.get_current_spark_version() instead.")
    def get_current_spark_version(self, client=None):
        if self.includes_dbrest:
            # noinspection PyUnresolvedReferences
            from dbacademy import dbrest
            cluster_id = self.get_tags()["clusterId"]
            client = dbrest.DBAcademyRestClient() if client is None else client
            cluster = client.clusters().get(cluster_id)
            return cluster.get("spark_version", None)

        else:
            raise Exception(f"Cannot use rest API with-out including dbacademy.dbrest")

    @deprecated(reason="Use dbacademy.dbrest.clusters.get_current_instance_pool_id() instead.")
    def get_current_instance_pool_id(self, client=None):
        if self.includes_dbrest:
            # noinspection PyUnresolvedReferences
            from dbacademy import dbrest
            cluster_id = self.get_tags()["clusterId"]
            client = dbrest.DBAcademyRestClient() if client is None else client
            cluster = client.clusters().get(cluster_id)
            return cluster.get("instance_pool_id", None)

        else:
            raise Exception(f"Cannot use rest API with-out including dbacademy.dbrest")

    @deprecated(reason="Use dbacademy.dbrest.clusters.get_current_node_type_id() instead.")
    def get_current_node_type_id(self, client=None):

        if self.includes_dbrest:
            # noinspection PyUnresolvedReferences
            from dbacademy import dbrest
            cluster_id = self.get_tags()["clusterId"]
            client = dbrest.DBAcademyRestClient() if client is None else client
            cluster = client.clusters().get(cluster_id)
            return cluster.get("node_type_id", None)

        else:
            raise Exception(f"Cannot use rest API with-out including dbacademy.dbrest")

    def proof_of_life(self,
                      expected_get_username,
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

        assert isinstance(self.dbutils, dbruntime.dbutils.DBUtils), f"Expected {dbruntime.dbutils.DBUtils}, found {type(self.dbutils)}"

        assert isinstance(self.spark, pyspark.sql.SparkSession), f"Expected {pyspark.sql.SparkSession}, found {type(self.spark)}"

        assert isinstance(self.sc, pyspark.context.SparkContext), f"Expected {pyspark.context.SparkContext}, found {type(self.sc)}"

        value = self.get_parameter("some_widget", default_value="undefined")
        assert value == "undefined", f"Expected \"undefined\", found \"{value}\"."

        value = self.get_cloud()
        assert value == "AWS", f"Expected \"AWS\", found \"{value}\"."

        value = self.get_tags()
        assert type(value) == JavaMap, f"Expected type \"dict\", found \"{type(value)}\"."

        value = self.get_tag("orgId")
        assert value == expected_get_tag, f"Expected \"{expected_get_tag}\", found \"{value}\"."

        value = self.get_username()
        assert value == expected_get_username, f"Expected \"{expected_get_username}\", found \"{value}\"."

        value = self.get_browser_host_name()
        assert value == expected_get_browser_host_name, f"Expected \"{expected_get_browser_host_name}\", found \"{value}\"."

        value = self.get_job_id()
        assert value is None, f"Expected \"None\", found \"{value}\"."

        value = self.is_job()
        assert value is False, f"Expected \"{False}\", found \"{value}\"."

        value = self.get_workspace_id()
        assert value == expected_get_workspace_id, f"Expected \"{expected_get_workspace_id}\", found \"{value}\"."

        value = self.get_notebook_path()
        assert value == expected_get_notebook_path, f"Expected \"{expected_get_notebook_path}\", found \"{value}\"."

        value = self.get_notebook_name()
        assert value == expected_get_notebook_name, f"Expected \"{expected_get_notebook_name}\", found \"{value}\"."

        value = self.get_notebook_dir()
        assert value == expected_get_notebook_dir, f"Expected \"{expected_get_notebook_dir}\", found \"{value}\"."

        value = self.get_notebooks_api_endpoint()
        assert value == expected_get_notebooks_api_endpoint, f"Expected \"{expected_get_notebooks_api_endpoint}\", found \"{value}\"."

        value = self.get_notebooks_api_token()
        assert value is not None, f"Expected not-None."

        if not self.includes_dbrest:
            self.print_warning(title="DEPENDENCY ERROR", message="The methods get_current_spark_version(), get_current_instance_pool_id() and get_current_node_type_id() require inclusion of the dbacademy_rest libraries")
        else:
            value = self.get_current_spark_version()
            assert value == expected_get_current_spark_version, f"Expected \"{expected_get_current_spark_version}\", found \"{value}\"."

            value = self.get_current_instance_pool_id()
            assert value == expected_get_current_instance_pool_id, f"Expected \"{expected_get_current_instance_pool_id}\", found \"{value}\"."

            value = self.get_current_node_type_id()
            assert value == expected_get_current_node_type_id, f"Expected \"{expected_get_current_node_type_id}\", found \"{value}\"."

        print("All tests passed!")

    @staticmethod
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

    @staticmethod
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


dbgems: DBGems = DBGems()
