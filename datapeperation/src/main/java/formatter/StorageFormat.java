package formatter;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class StorageFormat {


      public void toCSV(Dataset<Row> df, String path){
         df.write().format("csv").option("header",true).save(path);
     }

      public void toORC(Dataset<Row> df, String path){
         df.write().format("orc").save(path);
     }

      public void toAvro(Dataset<Row> df, String path){
         df.write().format("avro").save(path);
     }

}
