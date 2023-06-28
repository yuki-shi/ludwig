from setuptools import find_packages, setup

setup(
    name="ludwig",
    packages=find_packages(exclude=["ludwig_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "requests",
        "beautifulsoup4",
        "selenium",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
