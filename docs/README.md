# PAPyA
### Performance Analysis of Large RDF Graphs Processing Made Easy

<p align="center">
<img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/papayalogo.png" width="100"/>
</p>


Prescriptive Performance Analysis in Python Actions 

[![DOI](https://zenodo.org/badge/487547762.svg)](https://zenodo.org/badge/latestdoi/487547762)

### PAPyA Architecture
This library provides prescriptive analysis for the complex solution space of (RDF relational schema, Partitioning, and Storage Formats) that emerges with querying large RDF graphs over Relational Big Data (BD) System, e.g., Apache Spark-SQL.

<p align="center">
<img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/systemArchitecture.png"/>
</p>

### PAPyA Components
#### Data Preparator
The module builds an exemplar pipeline for testing PAPyA Bench-Ranking module in the context of querying Big RDF datasets.<br>
> <span class = "small">&#42;this work was build on top of an existing work, check the link for more information - <a href="https://github.com/DataSystemsGroupUT/SPARKSQLRDFBenchmarking"><em>SPARKSQL RDF Benchmarking</em></a></span><br>

- <a href="https://datasystemsgrouput.github.io/PAPyA/Data-Preperator">Data Preparator</a> <br>

#### Executor
This is the system that is subject to the experimentation which offers an abstract API to be extended.
```python
from abc import ABC, abstractmethod
class Executor(ABC):
    @abstractmethod
    def run(self, experiment, runs, dataPath, logsPath):
        pass
```
Executor starts the execution pipeline in the external system and get the performance logs which are currently persists on a file system (i.e. _HDFS_)

#### Ranker
The module enables prescriptive performance analysis over complex big data solution space. It calculate rankings and obtain useful data visualizations which could help determine the best performing configuration of the performance logs from previous modules.<br>

- <a href="https://datasystemsgrouput.github.io/PAPyA/Bench-Ranker">Bench-Ranker</a> <br>

#### UI
This is a running code example of the PAPyA pipeline
```python
from PAPyA.Rank import SDRank
#(1) SD - Ranking Criteria
schemaSDRank = SDRank(config, logs, dataset, 'schemas').calculateRank() # schema SDranking, 100M dataset size
partitionSDRank = SDRank(config, logs, dataset, 'partition').calculateRank() # partition SDranking, 250M dataset size
storageSDRank = SDRank(config, logs, dataset, 'storage').calculateRank() # storage SDranking, 100M dataset size

from PAPyA.Rank import MDRank
#(2) MD - Ranking ( Pareto )
paretoFronts_Q = MDRank(config, logs, dataset).paretoQ()
paretoFronts_Agg = MDRank(config, logs, dataset).paretoAgg()

# Visualization
SDRank(config, logs, dataset, 'schemas').plot('horizontal') # plot SD ranking for schemas viewed by horizontal partitioning technique
MDRank(config, logs, dataset).plot() # plot MD ranking paretoAgg

from PAPyA.Ranker import Conformance
from PAPyA.Ranker import Coherence
# Ranking Validation
conformance_set = ['schemas', 'partition', 'storage', 'paretoQ', 'paretoAgg']
coherence_set = ['schemas', 'partition', 'storage', 'paretoQ', 'paretoAgg']

conf = Conformance(config, logs, dataset, conformance_set, k_value, h_value)
coh = Coherence(config, logs,conformance_set, rankset1, rankset2)
```

In <a href="https://github.com/DataSystemsGroupUT/PAPyA/blob/main/BenchRanker/BenchRanker.ipynb">notebook</a>, we provide a pre-run cells of the jupyter notebook along with the configuration file in <a href="https://github.com/DataSystemsGroupUT/PAPyA/blob/main/BenchRanker/settings.yaml">settings.yaml</a> to perform the analysis over the provided performance logs from the _Data Preparator_ module.

### PAPyA Impact, Design Aspects, Avaiblability and Resuability 

#### Impact

| Impact  | Description |
| ------------- | ------------- |
| Does PAPyA break new ground? | To best of our knowledge, PAPyA is the first effort to enable prescriptive analysis in a form and automated and extensible tool |
| Does PAPyA fill an important gap? | Yes, PAPyA helps reduce the time needed to perform a prescriptive performance analysis of BD engine |
|How does PAPyA advance the state of the art?|PAPYA provides an automation for the techniques we offered in our previous work for providing prescriptive analyis for BD.|
|Has PAPyA been compared to other existing resources (if any) of similar scope?|Yes, there are other tools that aim at automating the pipleionies of BD, but to the best of our knowledge, none of these frameworks applies prescriptive performance for BD analysis|
|Is PAPyA of interest to the Semantic Web community?|Yes, PAPyA uses querying large RDF graphs on top of relational engines as an exmplar for showing complex solution space, that prescriptive analysis going to directly impact|
|Is PAPyA of interest to society in general?|Yes, PAPyA can be of interest for society in general but it is mainly used for BigData practitioners|
|Will/has PAPyA have/had an impact, especially in supporting the adoption of Semantic Web technologies?|Yes, with Papaya practitioners can easily choose the best-performing configurations in an experimental setup of processing large RDF graphs, as well as setting up the pipeline using our automated Data Preperator.|

#### Reusability

| Reusability  | Description |
| ------------- | ------------- |
|Is there evidence of usage by a wider community beyond PAPyA creators or their project? Alternatively (for new resources), what is PAPyA's potential for being (re)used; for example, based on the activity volume on discussion fora, mailing lists, issue trackers, support portal, etc?|PAPyA can be used from practitioners of two communities, big data and the semantic web|
|Is PAPyA easy to (re)use? For example, does it have high-quality documentation? Are there tutorials available?|Yes, we provide a github webpage for the resource <a href="https://datasystemsgrouput.github.io/PAPyA/">PAPyA</a>|
|Is PAPyA general enough to be applied in a wider set of scenarios, not just for the originally designed use? If it is specific, is there substantial demand.|PAPyA is extensible from both programming architecture and abstractions, therefore making it easy to reuse on a new project|
|Is there potential for extensibility to meet future requirements?|Yes, the system is extensible in terms of architectures and programming abstractions|
|Does PAPyA include a clear explanation of how others use the data and software? Or (for new resources) how others are expected to use the data and software?|Yes, we have a good documentation for using PAPyA along with jupyter notebook with running examples on how to use it|
|Does PAPyA description clearly state what it can and cannot do, and the rationale for the exclusion of some functionality?|Yes, We explained that in our paper, and in each of the modules (e.g., BenchRanker, and Data Preperator)|

#### Design & Technical Quality

| Design  | Description |
| ------------- | ------------- |
|Does the design of PAPyA follow resource-specific best practices?|We designed PAPyA to use standard python libraries and also enable abstractions for extending|
|Did the authors perform an appropriate reuse or extension of suitable high-quality resources? For example, in the case of ontologies, authors might extend upper ontologies and/or reuse ontology design patterns.|PAPyA uses standard ranking functions using standard python libraries (we don't use other external resources)|
|Is PAPyA suitable for solving the task at hand?|Yes, PAPyA helps automating the perscriptive analysis for big data relational engines|
|Does PAPyA provide an appropriate description (both human- and machine-readable), thus encouraging the adoption of FAIR principles? Is there a schema diagram? For datasets, is the description available in terms of VoID/DCAT/DublinCore?|We provide examples of relational RDF datasets along with descriptions of logical and physical storage options (in data preparator module)|

#### Availability

| Availability  | Description |
| ------------- | ------------- |
|Is PAPyA (and related results) published at a persistent URI (PURL, DOI, w3id)?|Yes, [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.6509898.svg)](https://doi.org/10.5281/zenodo.6509898)|
|Is there a canonical citation associated with PAPyA?|We keep better version of the library in Zenodo|
|Does PAPyA provide a licence specification? (See creativecommons.org, opensource.org for more information)|Yes, PAPyA is an open source library under MIT license|
|Is PAPyA publicly available? For example as API, Linked Open Data, Download, Open Code Repository.|Yes, PAPyA is published at persistent URI on the GitHub repository as an open-source library|
|Is PAPyA publicly findable? Is it registered in (community) registries (e.g. Linked Open Vocabularies, BioPortal, or DataHub)? Is it registered in generic repositories such as FigShare, Zenodo or GitHub?|PAPyA is registered in the GitHub repository|
|Is there a sustainability plan specified for PAPyA? Is there a plan for the medium and long-term maintenance of PAPyA?|- provide support of SPARQL by incorporating native triple stores for query evaluation<br>- incorporate SPARQL into SQL translation using advancement of R2RML mapping tool<br>- wraps other SQL on Hadoop executors to PAPyA<br>- use orchestration tools such as Apache Airflow to run PAPyA pipelines<br>- integrate PAPyA with other tools like gmark that generates graphs and workloads for distributed relational setups|
