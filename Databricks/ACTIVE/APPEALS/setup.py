from setuptools import setup, find_packages
# setup(packages=find_packages(include = ['active_common_utils'], install_requires=['pyspark.sql.functions']))
setup(
    name = 'active_shared_functions', version = '0.1', py_modules=['active_common_utils']
)
