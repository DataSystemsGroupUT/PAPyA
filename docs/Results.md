# Results
This section presents PAPyA library in practice, we focus on the performance of a generalized single dimension and multi dimension ranking for complex big data solutions using a perscriptive performance analysis (PPA) approach. Our experiment design consist of:

- set of queries which are manually translated in SQL from SPARQL
- RDF datasets of different sizes 
- A configuration file based on the three dimensions (i.e. schemas, partitioning, and storage formats)

In our experiment, we evaluate the performance of SparkSQL as a relational engine for evaluating the query workload. In particular, our key performance index is the query latency, but this could be extended to other metrics of evaluation. Our analysis is based on the average result of five different runs.

### Data Preparator Part

### Bench-Ranking
Bench-Ranking phase starts when we have results from Data Preparator in log files in the log folder of our repository. To start the analysis, we need to specify all dimensions and their options along with our key performance index which in our case is the query runs.

```python
# configuration file
dimensions:
    schemas: ["st", "vt", "pt", "extvt", "wpt"]
    partition: ["horizontal", "predicate", "subject"]
    storage: ["csv", "avro", "parquet", "orc"]
query: 11
```
```python
# log file structures
log
└───100M
|    │   st.horizontal.csv.txt
|    │   st.horizontal.avro.txt
|    │   ...
│
└───250M
|     |   st.horizontal.csv.txt
|     │   st.horizontal.avro.txt
|     │   ...
```

In this experiment, we could get the single dimension ranking scores by calling the calculateRank function from the SDRank class which needs 4 parameters, the config file, logs file, dataset size, and the dimension we want to be ranked

```python
from PAPyA.Rank import SDRank

SDRank(config, logs, '100M', 'schemas').calculateRank()
```
<p align="center">
<img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/top5schemaRank.png"/>
</p>
The user can plot individual rank scores by calling the plot method from the single dimension class.

```python
SDRank(config, logs, '100M', 'schemas').plot('csv')
```
<p align="center">
<img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/schemaDigram.png"/>
</p>

To get the configuration solutions of multi dimensional rankings, we used paretoQ and paretoAgg method to call the two types of multi dimensional rankings respectively. This class takes three arguments, the config file, logs file, and the dataset size of our experiments.

```python
from PAPyA.Rank import MDRank

MDRank(config, logs, '100M').paretoQ()
MDRank(config, logs, '100M').paretoAgg()
```
<figure align = "center">
  <img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/paretoQSolution.png">
  <img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/paretoAggSolution.png">
  <figcaption align = "center">
  <em>Multi Dimensional Ranking Solutions</em>
  </figcaption>
</figure>
<br>
Users can plot paretoAgg method by calling the plot method from the MDRank class.

```python
MDRank(config, logs, '100M').plot()
```
<p align="center">
<img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/paretoDiagram.png"/>
</p>

Lastly, the library provides two metrics of evaluation to evaluate the goodness of the ranking criteria which are the conformance and coherence which can be called from the validator class.

```python
from PAPyA.Ranker import Conformance, Coherence

conformance_set = ['schemas', 'partition', 'storage', 'paretoQ', 'paretoAgg']
coherence_set = ['schemas', 'partition', 'storage', 'paretoQ', 'paretoAgg']

Conformance(config, logs, '100M', conformance_set, 5, 28).run()
Coherence(config, logs, coherence_set, '100M', '250M').run()
```
<figure align = "center">
  <img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/conformanceScore.png">
  <img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/coherenceScore.png">
  <figcaption align = "center">
  <em>Ranking Validation Scores</em>
  </figcaption>
</figure>
<br>
Both of these methods can take a list of ranking criterions the users want to evaluate

### Performance Analysis
<table>
  <tr>
    <th></th>
    <th>100M</th>
    <th>250M</th>
  </tr>
  <tr>
    <th>SD Storage</th>
    <td>st.predicate.orc</td>
    <td>st.predicate.orc</td>
  </tr>
  <tr>
    <th>SD Partition</th>
    <td>st.subject.parquet</td>
    <td>vt.subject.orc</td>
  </tr>
  <tr>
    <th>SD Schema</th>
    <td>st.predicate.orc</td>
    <td>st.predicate.orc</td>
  </tr>
  <tr>
    <th>ParetoAgg</th>
    <td>pt.subject.csv</td>
    <td>vt.predicate.parquet</td>
  </tr>
  <tr>
    <th>ParetoQ</th>
    <td>wpt.subject.orc</td>
    <td>vt.subject.parquet</td>
  </tr>
</table>

This table shows the top ranked configurations for each ranking criteria (i.e. Single Dimension and Multi Dimension Ranking) for 100M and 250M datasets.<br>

<!-- <table>
    <tr>
        <td colspan="2">Three</td>
        <td>Conformance</td>
    </tr>
    <tr>
        <td>One</td>
        <td>Two</td>
    </tr>
</table> -->