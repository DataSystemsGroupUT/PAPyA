from setuptools import setup, find_packages
import codecs
import os

here = os.path.abspath(os.path.dirname(__file__))

with codecs.open(os.path.join(here, "README.md"), encoding="utf-8") as fh:
    long_description = "\n" + fh.read()

VERSION = '1.1.6'
DESCRIPTION = 'A library for performance analysis of sql based RDF graph processing systems'
LONG_DESCRIPTION = 'PAPyA simplifies the performance analysis of big data systems for processing large RDF graphs. This is an open-source library under an MIT license that will be a catalyst for designing new research prescriptive analysis techniques for big data applications'

# Setting up
setup(
    name="PAPyA",
    version=VERSION,
    author="Mohammed Ragab, Adam Satria Adidarma, Riccardo Tommasini",
    author_email="mohammed.ragab@ut.ee, adam.19051@mhs.its.ac.id, riccardo.tommasini@ut.ee",
    description=DESCRIPTION,
    long_description_content_type="text/markdown",
    long_description=long_description,
    packages=find_packages(),
    install_requires=['matplotlib', 'notebook', 'PyYAML', 'pandas', 'scipy', 'plotly', 'seaborn'],
    keywords=['python', 'benchmarking', 'rdf systems', 'big data', 'apache spark'],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Operating System :: Unix",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
    ]
)