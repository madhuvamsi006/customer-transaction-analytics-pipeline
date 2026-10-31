from setuptools import find_packages, setup

setup(
    name="customer-transaction-analytics",
    version="1.0.0",
    description="Metadata-driven Bronze→Silver→Gold pipeline for customer transaction analytics on GCP",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.9",
    install_requires=[
        "apache-beam[gcp]>=2.55.0",
        "pyspark>=3.5.0",
        "google-cloud-bigquery>=3.14.0",
        "google-cloud-storage>=2.14.0",
        "pyyaml>=6.0.1",
    ],
    extras_require={
        "dev": ["pytest>=8.0.0", "pytest-cov>=4.1.0"],
    },
)
