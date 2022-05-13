# Data Preperator

### Intro
This module generates an exemplare pipeline for testing PAPyA Bench-Ranking for querying big RDF datasets scenario which takes an input of RDF graph encoded in _N-Triple_ serialization. Data Preparator allows defining an arbitrary number of dimensions with as many options as necessary. In this experiment we have three dimensions specified (i.e. relational schemas, partitioning techniques, and storage format). Therefore, Data Preparator will automatically generates the relational schemas for the input RDF dataset according to the specified configurations. Data Preparator's interface is generic, and the generated data is _agnostic_ to the underlying relational system. Current implementation of the system relies on _SparkSQL_, allowing RDF relational schema generation using SQL transformation. SparkSQL also supports different partitioning techniques and multiple storage formats, making it ideal for our experiments.

<p>
<img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/dataPreparator.png"/>
</p>

This figure shows example of schema generation in Data Preparator module. First, the Data Preparator transforms the input RDF graph into an Single Statement schema, and then other schemas are generated using parameterized SQL queries. For example, Vertical-Partitioned schema and Wide Property Table schema are generated using SQL queries against the Single Statement table. While, Extended Vertical-Partitioned schema generation relies on Vertical-Partitioned schema to exist first.

### Relational Schema
Currently, Data Preparator includes three relational schemas commonly used in RDF processing:
- Single Statement (ST)
-> storing triples using a ternary relation (subject, predicate, object), which often requires many self-joins
- Vertical-Partitioned Table (VP)
-> mitigate some issues of self-joins in ST schema by using binary relations (subject, object) for each unique predicate in dataset
- Wide Property Table (WPT)
-> attempts to encode the entire dataset into a single denormalized table
- Extended Vertical-Partitioned Table (ExtVP)
-> precomputes semi-joins VP tables to reduce data shuffling