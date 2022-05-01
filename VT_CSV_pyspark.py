from pyspark.sql import SparkSession
from partitions.VTPartition import VTPartition
from queries.VTQuery import VTQuery
import time
import sys

#choose partition
partition = input("Choose partition: ")
VTPartition(partition.lower())

#starting spark session & load data
spark = SparkSession.builder.appName("bench-ranking").getOrCreate()

if(partition.lower() == 'predicate'):
    df_abstract = spark.read.load("./data/CSV_data/VT_csv/abstract.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_booktitle = spark.read.load("./data/CSV_data/VT_csv/booktitle.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_creator = spark.read.load("./data/CSV_data/VT_csv/creator.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_editor = spark.read.load("./data/CSV_data/VT_csv/editor.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_homepage = spark.read.load("./data/CSV_data/VT_csv/homepage.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_injournal = spark.read.load("./data/CSV_data/VT_csv/injournal.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_issued = spark.read.load("./data/CSV_data/VT_csv/issued.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_name = spark.read.load("./data/CSV_data/VT_csv/name.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_pages = spark.read.load("./data/CSV_data/VT_csv/pages.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_partof = spark.read.load("./data/CSV_data/VT_csv/partof.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_referencesV = spark.read.load("./data/CSV_data/VT_csv/references.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_references = spark.read.load("./data/CSV_data/PT_csv/Reference.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_seealso = spark.read.load("./data/CSV_data/VT_csv/seealso.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_subclassof = spark.read.load("./data/CSV_data/VT_csv/subclassof.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_title = spark.read.load("./data/CSV_data/VT_csv/title.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_type = spark.read.load("./data/CSV_data/VT_csv/type.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_predicatesCombined = spark.read.load("./data/CSV_data/ST_csv/SingleStmtTable100K.csv", format="csv", sep=",", header="true", inferSchema="true")
    
    df_abstract.createOrReplaceTempView("abstractv")
    df_booktitle.createOrReplaceTempView("booktitle")
    df_creator.createOrReplaceTempView("creator")
    df_homepage.createOrReplaceTempView("homePage")
    df_injournal.createOrReplaceTempView("journal")
    df_issued.createOrReplaceTempView("issued")
    df_name.createOrReplaceTempView("name") 
    df_pages.createOrReplaceTempView("pages")
    df_partof.createOrReplaceTempView("partOf")
    df_references.createOrReplaceTempView("references")
    df_referencesV.createOrReplaceTempView("referencesv")
    df_seealso.createOrReplaceTempView("seeAlso")
    df_subclassof.createOrReplaceTempView("subClassOf")
    df_title.createOrReplaceTempView("title")
    df_type.createOrReplaceTempView("type")
    df_predicatesCombined.createOrReplaceTempView("SingleStmtTable")
    df_editor.createOrReplaceTempView("editorv")

else:
    df_abstract = spark.read.load(f"./data/CSV_data/VT_csv/abstract{partition}.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_booktitle = spark.read.load(f"./data/CSV_data/VT_csv/booktitle{partition}.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_creator = spark.read.load(f"./data/CSV_data/VT_csv/creator{partition}.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_editor = spark.read.load(f"./data/CSV_data/VT_csv/editor{partition}.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_homepage = spark.read.load(f"./data/CSV_data/VT_csv/homepage{partition}.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_injournal = spark.read.load(f"./data/CSV_data/VT_csv/injournal{partition}.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_issued = spark.read.load(f"./data/CSV_data/VT_csv/issued{partition}.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_name = spark.read.load(f"./data/CSV_data/VT_csv/name{partition}.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_pages = spark.read.load(f"./data/CSV_data/VT_csv/pages{partition}.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_partof = spark.read.load(f"./data/CSV_data/VT_csv/partof{partition}.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_referencesV = spark.read.load(f"./data/CSV_data/VT_csv/references{partition}.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_references = spark.read.load(f"./data/CSV_data/PT_csv/Reference{partition}.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_seealso = spark.read.load(f"./data/CSV_data/VT_csv/seealso{partition}.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_subclassof = spark.read.load(f"./data/CSV_data/VT_csv/subclassof{partition}.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_title = spark.read.load(f"./data/CSV_data/VT_csv/title{partition}.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_type = spark.read.load(f"./data/CSV_data/VT_csv/type{partition}.csv", format="csv", sep=",", header="true", inferSchema="true")
    df_predicatesCombined = spark.read.load(f"./data/CSV_data/ST_csv/SingleStmtTable100K{partition}.csv", format="csv", sep=",", header="true", inferSchema="true")

    df_abstract.createOrReplaceTempView("abstractv")
    df_booktitle.createOrReplaceTempView("booktitle")
    df_creator.createOrReplaceTempView("creator")
    df_homepage.createOrReplaceTempView("homePage")
    df_injournal.createOrReplaceTempView("journal")
    df_issued.createOrReplaceTempView("issued")
    df_name.createOrReplaceTempView("name") 
    df_pages.createOrReplaceTempView("pages")
    df_partof.createOrReplaceTempView("partOf")
    df_references.createOrReplaceTempView("reference")
    df_referencesV.createOrReplaceTempView("referencesv")
    df_seealso.createOrReplaceTempView("seeAlso")
    df_subclassof.createOrReplaceTempView("subClassOf")
    df_title.createOrReplaceTempView("title")
    df_type.createOrReplaceTempView("type")
    df_predicatesCombined.createOrReplaceTempView("SingleStmtTable")
    df_editor.createOrReplaceTempView("editorv")

query = [VTQuery().q1, VTQuery().q2, VTQuery().q3, VTQuery().q4, VTQuery().q5, VTQuery().q6, VTQuery().q7, VTQuery().q8, VTQuery().q9, VTQuery().q10, VTQuery().q11]
counter = 1
for i in query:
  start_time = time.time()
  df2 = spark.sql(i).show()
  timer = time.time() - start_time

  original_stdout = sys.stdout # Save a reference to the original standard output
  with open('log.txt', 'a') as f:
      sys.stdout = f # Change the standard output to the file we created.
      print("QUERY " + str(counter) + " = " + str(timer) + " ")
      sys.stdout = original_stdout # Reset the standard output to its original value
      counter += 1

print("all  queries are done -csv -vt")