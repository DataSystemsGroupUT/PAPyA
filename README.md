<p align="center">
<a href='https://datasystemsgrouput.github.io/PAPyA/'><img src="https://github.com/DataSystemsGroupUT/PAPyA/blob/main/figs/papayalogo.png" width="100"/> </a>
</p>

# <a href='https://datasystemsgrouput.github.io/PAPyA/'>PAPyA</a>

Prescriptive Performance Analysis  in Python Actions 

[![DOI](https://zenodo.org/badge/487547762.svg)](https://zenodo.org/badge/latestdoi/487547762)

This library provides prescriptive analysis for the complex solution space of (RDF relational schema, Partitioning, and Storage Formats) that emerges with querying large RDF graphs over Relational Big Data (BD) System, e.g., Apache Spark-SQL.

* Documentation and more details about <a href='https://datasystemsgrouput.github.io/PAPyA/'> PAPya </a><br

## Installation

> PAPyA is uploaded to [PyPI](https://pypi.org/project/PAPyA/) package manager to help users find and install PAPyA easily in their environment. 

To use PAPyA on an environment, run: 
```
pip install PAPyA
```

Or clone this repo:
```
git clone https://github.com/DataSystemsGroupUT/PAPyA.git
```

## Experiments

Below are some examples of our running experiments using PAPyA library with different sets of configurations generated from ipython notebooks:<br>

When running the experiments, We used two different datasets to test replicability on the system namely [Watdiv](https://dsg.uwaterloo.ca/watdiv/) and [SP2Bench](https://arxiv.org/abs/0806.4627).

#### Watdiv
- [Full Experiment](./UI%20Module/Experiments/Full_Experiment_Watdiv.ipynb)<br>
Complete running experiment without removing any configurations.<br>
- [Mini](./UI%20Module/Experiments/Mini_Watdiv.ipynb) <br>
Remove some configurations in each dimensions (_schemas: {extvp, wpt} , partition: {predicate}, storage: {avro, csv}_).<br>
- [Single Partition](./UI%20Module/Experiments/Watdiv_Only_Horizontal.ipynb) <br>
Only having one partitioning technique (_horizontal_).<br>
- [No ExtVp & WPT](./UI%20Module/Experiments/Watdiv_Without_Extvp_Wpt.ipynb)
Remove _extvp_ and _wpt_ schemas from the configuration. <br>
#### SP2Bench
- [No Partition](./UI%20Module/Experiments/BenchRanker_twoDimensions.ipynb)<br>
Remove one dimension from the configuration (_partition_)