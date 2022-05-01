from pyspark.sql import SparkSession

class STPartition:
    def __init__(self, partitionType):
        self.partitionType = partitionType

        spark = SparkSession.builder.appName("bench-ranking").getOrCreate()
        df = spark.read.load("./data/CSV_data/ST_csv/SingleStmtTable100K.csv", format="csv", sep=",", header="true", inferSchema="true")

        if(partitionType == 'subject'):
            df.repartition(84, "Subject").write.option("header","true").format("csv").mode('overwrite').save(f"./data/CSV_data/ST_csv/SingleStmtTable100K{partitionType}.csv")
            print("CSV ST created, subject")

        elif(partitionType == 'predicate'):
            df.repartition(84, "Predicate").write.option("header","true").format("csv").mode('overwrite').save(f"./data/CSV_data/ST_csv/SingleStmtTable100K{partitionType}.csv")
            print("CSV ST created, predicate")
        
        elif(partitionType == 'horizontal'):
            df.repartition(84).write.option("header", "true").format("csv").mode('overwrite').save(f"./data/CSV_data/ST_csv/SingleStmtTable100K{partitionType}.csv")
            print("CSV ST created, horizontal")

