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