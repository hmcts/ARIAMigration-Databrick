from setuptools import find_packages, setup

setup(
    name='ARIAFUNCTIONS',
    version='0.0.1',
    packages=find_packages(),
    description='These are custom functions that will be used for the ARIA data migration',
    author='Ara Islam + Naveen Sriram',
    install_requires = ['azure-eventhub',
                        'azure-storage-blob',
                        'confluent_kafka',
                        'aiohttp',
                        'numpy',
                        'pyspark']
)