import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="neptunedb",
    version="0.0.1",
    author="Rajan Subramanian",
    author_email="rs3166@columbia.edu",
    description="Library for connecting to proprietary data stores",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/mrajancsr/neptune-db",
    project_urls={"Bug Tracker": "https://github.com/mrajancsr/neptune-db/issues"},
    license="MIT",
    packages=["neptunedb"],
    install_requires=[
        "pandas",
        "psycopg2-binary",
        "paramiko",
        "sshtunnel",
        "dataclasses-json",
        "typing-inspect",
        "asyncpg",
        "typing_extensions",
    ],
    include_package_data=True,
    package_data={"": ["sql_config/*"]},
)
