import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="parcsv",
    version="0.0.1",
    author="Tim Stewart",
    author_email="tim.j.stewart@gmail.com",
    description="Map functions over a csv on multiple cores.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/timjstewart/parcsv",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
