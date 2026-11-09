"""
Setup configuration for E-Commerce Demand-Supply Spark package.
"""
from setuptools import setup, find_packages

setup(
    name="ecommerce-demand-supply-spark",
    version="1.0.0",
    author="Sagnik De",
    author_email="sagnikde191@gmail.com",
    description="A distributed e-commerce demand-supply analytics system built with Apache Spark",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/sagnikde16/ecommerce-demand-supply-spark",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.10",
    install_requires=[
        "pyspark>=3.5.0",
        "delta-spark>=3.1.0",
        "fastapi>=0.111.0",
        "streamlit>=1.35.0",
        "pandas>=2.2.0",
        "numpy>=1.26.0",
        "pydantic>=2.7.0",
        "loguru>=0.7.0",
    ],
    classifiers=[
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Database :: Database Engines/Servers",
    ],
    entry_points={
        "console_scripts": [
            "ecomm-etl=src.processing.etl_pipeline:main",
            "ecomm-api=src.api.main:start",
            "ecomm-generate=scripts.generate_data:main",
        ]
    },
)
