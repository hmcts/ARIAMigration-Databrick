from setuptools import find_packages, setup

setup(
    name='shared_functions',
    version='0.0.2',
    packages=find_packages(),
    description='These are custom functions that will be used for the ARIA data migration',
    author='Ara Islam + Naveen Sriram + Andrew McDevitt + Graham Burnside',
    install_requires = ['azure-eventhub',
                        'azure-storage-blob',
                        'confluent_kafka',
                        'aiohttp',
                        'numpy',
                        'pyspark']
)