from setuptools import find_packages, setup

with open("requirements.txt") as f:
    requirements = f.read()

setup(
    name='ARIAFUNCTIONS',
    version='0.0.1',
    packages=find_packages(),
    description='These are custom functions that will be used for the ARIA data migration',
    author='Ara Islam + Naveen Sriram',
    entry_points={
        "packages": [
            "main=wheel_package.main:main"
        ]
    },
    install_requires=[]
)

