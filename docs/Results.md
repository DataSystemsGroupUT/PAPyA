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
dimensions:
    schemas: ["st", "vt", "pt", "extvt", "wpt"]
    partition: ["horizontal", "predicate", "subject"]
    storage: ["csv", "avro", "parquet", "orc"]
query: 11
```

In this experiment, we could get the single dimension ranking scores by calling the calculateRankScore which has two parameters, the single dimension and dataset sizes.

```python
import Ranker

Ranker.SDRank()calculateRankScore('schemas', '100M')
```

The user can plot individual rank scores by calling the plot method from the single dimension class.

```python
Ranker.SDRank().plot('schemas', '100M', 'csv')
```

To get the configuration solutions of multi dimensional rankings, we used paretoQ and paretoAgg method to call the two types of multi dimensional rankings respectively. This method takes one argument, the dataset sizes.

```python
import Ranker

Ranker.MDRank().paretoQ('250M')
Ranker.MDRank().paretoAgg('100M')
```

Users can plot paretoAgg method by calling the plot method from the MDRank class.

```python
Ranker.MDRank().plot('100M')
```

Lastly, the library provides two metrics of evaluation to evaluate the goodness of the ranking criteria which are the conformance and coherence which can be called from the validator class.

```python
import Ranker

conformance_set = ['schemas', 'partition', 'storage', 'paretoQ', 'paretoAgg']
coherence_set = ['schemas', 'partition', 'storage', 'paretoQ', 'paretoAgg']

Ranker.Validator().conformance(conformance_set, '100M', 5, 28)
Ranker.Validator().coherence(coherence_set, '100M', '250M')
```

Both of these methods can take a list of ranking criterions the users want to evaluate

### Performance Analysis
|| 100M  | 250M |
| ------------- | ------------- | ------------- |
|SD Storage|st.predicate.orc|st.predicate.orc|
|SD Partition|st.subject.parquet|vt.subject.orc|
|SD Schema|st.predicate.orc|st.predicate.orc|
|ParetoAgg|pt.subject.csv|vt.predicate.parquet|
|ParetoQ|wpt.subject.orc|vt.subject.parquet|