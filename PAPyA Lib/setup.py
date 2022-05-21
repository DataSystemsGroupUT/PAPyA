from setuptools import setup, find_packages

def readme():
    with open('README.md') as f:
        README = f.read()
    return README

# Setting up
setup(
    name="PAPyA",
    version="0.1.0",
    author="Data Systems Group",
    author_email="<mail@testing.com>",
    description="Prescriptive Performance Analysis for Big Data Problems",
    long_description=readme(),
    long_description_content_type="text/markdown",
    url="https://datasystemsgrouput.github.io/PAPyA/",
    license="MIT",
    packages=['PAPyA'],
    include_package_data=True,
    install_requires=['PyYAML', 'plotly', 'pandas', 'matplotlib', 'scipy', 'numpy'],
    keywords=['python', 'Benchmarking', 'RDF', 'Big Data', 'Analytics'],
    classifiers=[
        "Development Status :: 1 - Planning",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Operating System :: Unix",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
    ]
)