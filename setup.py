# Databricks notebook source
import setuptools
from setuptools import find_packages

setuptools.setup(
    name="dbacademy-gems",
    version="0.1",
    package_dir={"dbacademy": "src"},
    packages=["dbacademy.dbgems"],
)
