from setuptools import setup
import setuptools

with open("./README.md", 'r') as f:
    long_description = f.read()

setup(
    name='yetl',
    version='1.0',
    description='Etl as Yaml',
    long_description=long_description,
    url="https://github.com/berbuf/yetl",
    packages=["yetl", "yetl/etl", "yetl/resolve", "yetl/yetl_functions"],
)
