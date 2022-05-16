package partitioner;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Partitioner {

      public Dataset<Row> partitionHorizontal(Dataset<Row> df, int numPartions){
        return  df.repartition(numPartions);
     }

      public Dataset<Row> partitionBySubject(Dataset<Row> df,  int numPartions){
      return   df.repartition(numPartions, df.col("s"));
     }

      public Dataset<Row> partitionByPredicate(Dataset<Row> df,  int numPartions){
       return df.repartition(numPartions,df.col("p"));
     }

}


