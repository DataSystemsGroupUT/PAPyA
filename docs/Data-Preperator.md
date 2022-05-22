# Data Preperator

### Intro
This module generates an exemplare pipeline for testing PAPyA Bench-Ranking for querying big RDF datasets scenario which takes an input of RDF graph encoded in _N-Triple_ serialization. Data Preparator allows defining an arbitrary number of dimensions with as many options as necessary. In this experiment we have three dimensions specified (i.e. relational schemas, partitioning techniques, and storage format). Therefore, Data Preparator will automatically generates the relational schemas for the input RDF dataset according to the specified configurations. Data Preparator's interface is generic, and the generated data is _agnostic_ to the underlying relational system. Current implementation of the system relies on _SparkSQL_, allowing RDF relational schema generation using SQL transformation. SparkSQL also supports different partitioning techniques and multiple storage formats, making it ideal for our experiments.

<p>
<img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/dataPreparator.png"/>
</p>

This figure shows example of schema generation in Data Preparator module. First, the Data Preparator transforms the input RDF graph into an Single Statement schema, and then other schemas are generated using parameterized SQL queries. For example, Vertical-Partitioned schema and Wide Property Table schema are generated using SQL queries against the Single Statement table. While, Extended Vertical-Partitioned schema generation relies on Vertical-Partitioned schema to exist first.

### Relational Schemas
Currently, Data Preparator includes four relational schemas commonly used in RDF processing:
- __Single Statement (ST)__ <br>
: storing triples using a ternary relation (subject, predicate, object), which often requires many self-joins <br><img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/st_table.png"/>
- __Vertical-Partitioned Table (VP)__ <br>
: mitigate some issues of self-joins in ST schema by using binary relations (subject, object) for each unique predicate in dataset <br><img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/vp_table.png"/>
- __Wide Property Table (WPT)__ <br>
: attempts to encode the entire dataset into a single denormalized table <br><img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/wpt_table.png"/>
- __Extended Vertical-Partitioned Table (ExtVP)__ <br>
: precomputes semi-joins VP tables to reduce data shuffling <br><img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/extvp_table.png"/>

### Partitioning Techniques
Data Preparator supports three different partitioning techniques: <br>
<img src="https://github.com/DataSystemsGroupUT/PAPyA/raw/main/figs/partition_table.png"/>
<ol type="a">
  <li>Horizontal Partitioning</li>
  : divides data evenly over the number of machines in the cluster
  <li>Subject Based Partitioning</li>
  : divides data across partitions according to the subject keys
  <li>Predicate Based Partitioning</li>
  : distribute data across various partitions according to the hash value computed for the predicate keys
</ol>
<!-- - Horizontal Partitioning <br>
: divides data evenly over the number of machines in the cluster  
- Subject Based Partitioning <br>
: divides data across partitions according to the subject keys
- Predicate Based Partitioning <br>
: distribute data across various partitions according to the hash value computed for the predicate keys -->

### Storage Formats
Data Preparator allows storing data using various HDFS file formats. In particular, the system has two types of storage format:
- Row-Store (_CSV_ and _Avro_) <br>
: storing data by record, keeping all of the data associated with a record next to each other in memory. Optimized for reading and writing rows efficiently
- Columnar-Store (_ORC_ and _Parquet_) <br>
: storing data by field, keeping all of the data associated with a field next to each other in memory. Optimized for reading and writing columns efficiently


### Getting Started with PAPyA Data Preperator:

For compiling and generating a ```jar``` with all dependenceies in DP module, one should run the following command inside the DP main directory:

```shell
mvn package assembly:single
```

For Directly Run the DP module, we uploaded the fat jar, into the ```target``` directory with the name (```PapyaDP-1.0-SNAPSHOT-jar-with-dependencies.jar```).


The user can specify the required schema, partioning techniques, and storage options  for his experiments inside using file ```loader-default.ini```.

Example: generating the logical partitioning, i.e., the relational schema:

```yaml
[logicalPartitioning]
TT		= false
WPT		= false
VP		= false
EXTVP   = false

[storage]
TTcsv=false
TTorc=false
TTavro=false
TTParquet=false

VPcsv=false
VPorc=false
VPavro=false
VPParquet=false

WPTcsv=false
WPTorc=false
WPTavro=false
WPTParquet=false

EXTVPcsv=false
EXTVPorc=false
EXTVPavro=false
EXTVPParquet=false

[physicalPartitioning]
TTp	= false
TTs	= false
TTh	= false

VPs=false
VPh=false

EXTVPs=false
EXTVPh=false

WPTs= false
WPTh= false

```


The data is generated by submmitting the jar to a spark job:

```shell
spark-submit --class run.PapyaDPMain  --master local[*] PapyaDP-1.0-SNAPSHOT-jar-with-dependencies.jar <OUTPUT_DIR> -db <dbName>  -i <RDF_SOURCE_DIR>
```
* The paramters of the job are as follows:
  * The dbname is the name of databse to be generated.
  * The <OUTPUT_DIR> is the output directory for the generated data.
  * The <RDF_SOURCE_DIR> is the source directory for the RDF (```N-Triples```), needed only for generating the ```ST``` schema. 

* *Note:* make sure that the ```loader-default.ini``` file should be loacated besides the jar file.
* The DP can directly generate the database into HDFS by specifying the ```<OUTPUT_DIR>``` as HDFS directory (```HDFS://...```). Otherwise, PAPyA DP loads the data locally (```FILE://...```).



