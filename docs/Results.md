# Results
This section presents PAPyA library in practice, we focus on the performance of a generalized single dimension and multi dimension ranking for complex big data solutions using a perscriptive performance analysis (PPA) approach. Our experiment design consist of :

- set of queries which are manually translated in SQL from SPARQL
- RDF datasets of different sizes 
- A configuration file based on the three dimensions (i.e. schemas, partitioning, and storage formats)

In our experiment, we evaluate the performance of SparkSQL as a relational engine for evaluating the query workload. In particular, our key performance index is the query latency, but this could be extended to other metrics of evaluation. Our analysis is based on the average result of five different runs.

## Bench-Ranking
Bench-Ranking phase starts when we have results from Data Preparator in log files in the log folder of our repository. To start the analysis, we need to specify all dimensions and their options along with our key performance index which in our case is the query runs.

```yaml
# configuration file
dimensions:
    schemas: ["st", "vt", "pt", "extvt", "wpt"]
    partition: ["horizontal", "predicate", "subject"]
    storage: ["csv", "avro", "parquet", "orc"]
query: 11
```
```xml
# log file structures
log
└───100M
     │   st.horizontal.csv.txt
     │   st.horizontal.avro.txt
     │   ...
└───250M
     |   st.horizontal.csv.txt
     │   st.horizontal.avro.txt
     │   ...
```

### Single Dimensional Ranking

We start the experiment by viewing our input data which are the log files of query runtimes over the configuration specified in the FileReader class parameters.
 ```python
from PAPyA.file_reader import FileReader

SDRank(config, logs, '100M', 'schemas').file_reader()
```
<p align="center">
<img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/query_runtimes.png"/>
</p>

In this experiment, we could get the single dimension ranking scores by calling the calculateRank function from the SDRank class which needs 4 parameters, the config file, logs file, dataset size, and the dimension we want to be ranked

```python
from PAPyA.Rank import SDRank

SDRank(config, logs, '100M', 'schemas').calculateRank()
```
<p align="center">
<img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/top5schemaRank.png"/>
</p>

We can have even more parameters within the calculateRank method to give different results when removing some queries in the configuration. 
```python
# String parameters to slice the schema in this case we slice it on predicate and csv. 
# While excluding a list of queries which in this case we exclude query 3,4, and 5.
SDRank(config, logs, '100M', 'schemas').calculateRank('predicate', 'csv', [3,4,5])
```
<p align="center">
<img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/sdrank_withParameters.png"/>
</p>

#### Single Dimensional Visualization
To represent user's data in an easy to read and interactive manner, PAPyA provides functionalities to visualize user's data to help rationalize the performance results and final decisions on their experimental data.
###### Radar Plot
Ranking over one dimension is insufficient when it counts multiple dimensions. The presence of trade-offs reduces the accuracy of single dimension ranking functions. This plot can help view and understand this problem in a simple and intuitive way.
```python
from PAPyA.Rank import SDRank
SDRank(config, logs, '100M', 'schemas').plotRadar()
```
<p align="center">
<img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/radarPlot.png"/>
</p>

This plot shows a figure of the top configuration of ranking by schema is optimized towards its dimension only, ignoring the other two dimensions.

###### Bar Plot
PAPyA also provides visualization that shows the performance of a single dimension parameters that user can choose in terms of their rank scores.
```python
SDRank(config, logs, '100M', 'schemas').plot('csv')
```
<p align="center">
<img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/schemaDigram.png"/>
</p>

###### Box Plot
In order to show the distributions of our query runtimes data, we need a box plot diagram to compare these data between queries in our experiment. Box plot can help provide information at a glance, giving us general information about our data.
```python
from PAPyA.Rank import SDRank

# Box plot example of query 1,2,3 of schema ranking dimension
SDRank(config, logs, '100M', 'schemas').plotBox(["Q1", "Q2", "Q3"])
```
<p align="center">
<img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/boxPlot2.png"/>
</p>

### Replicability

This library comes with the functionality to check the replicability of our system's performance when introducing different experimental dimensions. Replicability calculates over one dimensional option from the parameters over all the other dimensions.

```python
# mode 0, replicability on query ; mode 1, replicability on average
from PAPyA.Rank import SDRank
SDRank(config, logs, '100M', 'storage').replicability(options = 'csv', mode = 1)
```
<p align="center">
<img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/replicabilityResult.png"/>
</p>

On the example code above, we used the _storage_ dimension as the pivot point dimension to iterate over the other dimensions (_schemas_ and _partition_) with csv as the choosen option of storage.<br><br> Replicability has two modes to calculate over, the first one is to calculate from the query rankings while the other one calculates from the average of the single dimensional scores. <br><br>The result is a replicability scores for csv when changing parameters over the other dimensions.

#### Replicability Comparison 
PAPyA have a replicability comparison functionality to compare the replicability score of two options which the user can specify in the configuration files themselves. 
```yaml
# configuration file
dimensions:
    schemas: ["vp", "extvp"]
    partition: ["horizontal", "predicate", "subject"]
    storage: ["csv", "avro", "parquet", "orc"]
query: 11
```
```python
# comparing replicability of two options in schemas dimension. mode 0 to compare globally, mode 1 to compare locally
from PAPyA.Rank import SDRank
SDRank(config, logs, '100M', 'schemas').replicability_comparison(option = 'vp', mode = 1)
```
<p align="center">
<img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/replicabilityComparison.png"/>
</p>

The example above tries to compare _vp_ with _extvp_ in the schemas dimension while pivoting over all the options in the other dimensions (_partition_ and _storage_).<br><br>
This function has two modes of comparison, the first one is comparing it globally over the dimensions and the other one compares the option locally.

#### Replicability Visualization 
To make it easier to understand the impact of replicability over a dimensional options, this library provides a replicability plotting. In this example, we try to check the impact of storage dimension on the schemas dimension. 
```python
from PAPyA.Rank import SDRank
SDRank(config, logs, '100M', 'schemas').replicability_plot('storage', mode = 0)
```
<p align="center">
<img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/replicabilityPlot.png"/>
</p>

### RTA
Ranking by Triangle Area is a new ranking function we added to PAPyA to test its abstractions to add new user defined ranking criterion besides Single Dimensional and Multi Dimensional Ranking that we already provides. RTA calculates the area of triangle in which our three dimensional ranks are. The higher the score the better the configuration.
```python
from PAPyA.Rank import RTA
RTA(config, logs, '250M').rta()
```
<p align="center">
<img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/RTAResult.png"/>
</p>

<!-- ###################################################### -->
### Multi Dimensional Ranking
To get the configuration solutions of multi dimensional rankings, we apply the NSGA2 (Non-Dominated Sorting Genetic Algorithm) on paretoQ and paretoAgg method to call the two types of multi dimensional rankings respectively. This class takes three arguments, the config file, logs file, and the dataset size of our experiments.

```python
from PAPyA.Rank import MDRank

multiRanking = MDRank(config, logs, '100')
```
```python
multiRanking.paretoQ()
multiRanking.paretoAgg()
```
<figure align = "center">
  <img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/paretoQSolution.png">
  <img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/paretoAggSolution.png">
  <!-- <figcaption align = "center">
  <em>Multi Dimensional Ranking Solutions</em> -->
  <!-- </figcaption> -->
</figure>
<br>

- The first method is paretoQ which apply the algorithm considering the rank sets obtained by sorting each query results individually. This method aims at minimizing query runtimes of the ranked dimensions<br>
- The second method is paretoAgg which operates on the single dimensional ranking criteria. This method aims to maximize performance of the three ranks altogether<br>

#### Multi Dimensional Visualization
We can have a 3D visualization of multi dimensional ranking solution according to paretoAgg method as shades of green projected in the canvas.
```python
MDRank(config, logs, '100M').plot()
```
<p align="center">
<img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/paretoDiagram.png"/>
</p>

### Ranking Criteria Validation
Lastly, the library provides two metrics of evaluation to evaluate the goodness of the ranking criteria which are the conformance and coherence which can be called from the validator class. Both of these methods can take a list of ranking criterions the users want to evaluate.<br>
- Conformance measures the adherence of the top-ranked configurations according to the actual query positioning of thoses configurations.<br>
- Coherence is the measure agreement between two ranking sets that uses the same ranking criteria accross different experiments.<br>

```python
from PAPyA.Ranker import Conformance, Coherence

conformance_set = ['schemas', 'partition', 'storage', 'paretoQ', 'paretoAgg', 'RTA']
coherence_set = ['schemas', 'partition', 'storage', 'paretoQ', 'paretoAgg', 'RTA']

Conformance(config, logs, '250M', conformance_set, 5, 28).run()
Coherence(config, logs, coherence_set).run('100M'. '250M')
```
<figure align = "center">
  <img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/conformanceResult.png">
  <img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/coherenceResult.png">
  <!-- <figcaption align = "center">
  <em>Ranking Validation Scores</em>
  </figcaption> -->
</figure>
<br>

#### Ranking Criteria Validation Visualization
PAPyA also provides functionality to have visualizations for the ranking criteria validations. For conformance, we can check different ranking criterions performance using a bar plots. While for coherence, we have a heatmap plot to show the coherence between two particular ranking sets of user's choice. 
<b>Conformance</b><br>

- Bar Plot <br>
Conformance's bar plot has two modes of visualization. The first one is to have a global view of ranking criterions, the second, takes the three highest ranked configurations on each dimensions to plot.
```python
Conformance(config, logs, '250M', conformance_set, 5, 28).plot(0)
```
<p align="center">
<img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/conformanceGlobalPlot.png"/>
</p>

<b>Coherence</b><br>


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