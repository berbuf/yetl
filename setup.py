from setuptools import setup
import setuptools

with open("./README.md", 'r') as f:
    long_description = f.read()

setup(
    name='yetl',
    version='1.0',
    description='Etl as Yaml',
    license="MIT",
    long_description=long_description,
    author='BerBuf',
    author_email='berbuf@berbuf.com',
    url="https://github.com/berbuf/yetl",

    # same as name
    packages=["yetl", "yetl/etl", "yetl/resolve", "yetl/yetl_functions"],
    # packages=['yetl'],
)
