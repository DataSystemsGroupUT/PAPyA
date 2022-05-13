# PAPyA
### Performance Analysis of Large RDF Graphs Processing Made Easy

<p align="center">
<img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/papayalogo.png" width="100"/>
</p>


Prescriptive Performance Analysis in Python Actions 

[![DOI](https://zenodo.org/badge/487547762.svg)](https://zenodo.org/badge/latestdoi/487547762)

This library provides prescriptive analysis for the complex solution space of (RDF relational schema, Partitioning, and Storage Formats) that emerges with querying large RDF graphs over Relational Big Data (BD) System, e.g., Apache Spark-SQL.

<p align="center">
<img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/systemArchitecture.png"/>
</p>


#### PAPyA Impact, Design Aspects, Avaiblability and Resuability 

#### Impact

| Impact  | Description |
| ------------- | ------------- |
| Does the resource break new ground? | No, PAPyA is derived from previous paper |
| Does the resource fill an important gap? | Yes, PAPyA helps reduce the time needed to perform a prescriptive performance analysis of BD engine |
|How does the resource advance the state of the art?||
|Has the resource been compared to other existing resources (if any) of similar scope?|No, to the best of our knowledge currently there are no framework that applies the prescriptive performance approach of big data analysis|
|Is the resource of interest to the Semantic Web community?|Yes, PAPyA aims to automate performance analysis of big data solutions by using a prescriptive performance analysis approach|
|Is the resource of interest to society in general?|Yes, PAPyA can be of interest for society in general but it is mainly used for BigData practitioners|
|Will/has the resource have/had an impact, especially in supporting the adoption of Semantic Web technologies?|Yes, with Papaya practitioners can easily choose the best-performing configurations in an experimental setup of processing large RDF graphs, as well as setting up the pipeline using our automated Data Preperator.|
|Does the resource description clearly state what the resource can and cannot do, and the rationale for the exclusion of some functionality?|Yes, We explained that in our paper, and in each of the modules (e.g., BenchRanker, and Data Preperator)|

#### Reusability

| Reusability  | Description |
| ------------- | ------------- |
|Is there evidence of usage by a wider community beyond the resource creators or their project? Alternatively (for new resources), what is the resource’s potential for being (re)used; for example, based on the activity volume on discussion fora, mailing lists, issue trackers, support portal, etc?|PAPyA is extensible from both programming architecture and abstractions, therefore making it easy to reuse on a new project|
|Is the resource easy to (re)use? For example, does it have high-quality documentation? Are there tutorials available?|Yes, PAPya provides good example to run the experiments along with documentations on the system's webpage|
|Is the resource general enough to be applied in a wider set of scenarios, not just for the originally designed use? If it is specific, is there substantial demand.|PAPyA is independance from both the key performance indicator and their experimental dimensions, therefore applicable to any rank sets|
|Is there potential for extensibility to meet future requirements?|Yes, the system is extensible in terms of architectures and programming abstractions|
|Does the resource include a clear explanation of how others use the data and software? Or (for new resources) how others are expected to use the data and software?|Yes, PAPyA has explanation detailing how others could use the resource|
|Does the resource description clearly state what the resource can and cannot do, and the rationale for the exclusion of some functionality?|Yes|

#### Design & Technical Quality

| Design  | Description |
| ------------- | ------------- |
|Does the design of the resource follow resource-specific best practices?||
|Did the authors perform an appropriate reuse or extension of suitable high-quality resources? For example, in the case of ontologies, authors might extend upper ontologies and/or reuse ontology design patterns.||
|Is the resource suitable for solving the task at hand?||
|Does the resource provide an appropriate description (both human- and machine-readable), thus encouraging the adoption of FAIR principles? Is there a schema diagram? For datasets, is the description available in terms of VoID/DCAT/DublinCore?||

#### Reusability

| Design  | Description |
| ------------- | ------------- |
|Is the resource (and related results) published at a persistent URI (PURL, DOI, w3id)?||
|Is there a canonical citation associated with the resource?||
|Does the resource provide a licence specification? (See creativecommons.org, opensource.org for more information)|Yes, PAPyA is an open source library under MIT license|
|Is the resource publicly available? For example as API, Linked Open Data, Download, Open Code Repository.|Yes, PAPyA is published at persistent URI on the GitHub repository as an open-source library|
|Is the resource publicly findable? Is it registered in (community) registries (e.g. Linked Open Vocabularies, BioPortal, or DataHub)? Is it registered in generic repositories such as FigShare, Zenodo or GitHub?|PAPyA is registered in the GitHub repository|
|Is there a sustainability plan specified for the resource? Is there a plan for the medium and long-term maintenance of the resource?||
