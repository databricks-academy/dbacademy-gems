# Databricks notebook source
import setuptools
from setuptools import find_packages

setuptools.setup(
    name="dbacademy-gems",
    version="0.1",
    packages=['dbacademy'],
    package_dir={"dbacademy": "./dbacademy"},
)
