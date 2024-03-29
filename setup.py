import setuptools
from setuptools import find_packages


def find_dbacademy_packages():
    packages = find_packages(where="src")
    if "dbacademy" in packages: del packages[packages.index("dbacademy")]
    return packages


setuptools.setup(
    name="dbacademy-gems",
    author="Databricks, Inc",
    maintainer="Databricks Academy",
    version="v0.0.0",
    package_dir={"": "src"},
    packages=find_dbacademy_packages()
)
