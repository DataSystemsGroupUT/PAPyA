package run;

import com.sun.xml.internal.bind.v2.TODO;
import formatter.StorageFormat;
import loader.Settings;
import loader.TripleTableLoader;
import loader.VerticalPartitioningLoader;
import loader.WidePropertyTableLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import loader.extvp.*;
import org.apache.spark.sql.SparkSession;
import statistics.DatabaseStatistics;

import java.io.File;


public class Main {

    public static void main(String[] args) throws Exception {
        System.out.println("Ya karim");


        final Settings settings = new Settings(args);
        final DatabaseStatistics statistics;
        statistics = new DatabaseStatistics("watdiv");

        final SparkSession spark = settings.loadSparkSession();

        spark.sparkContext().setLogLevel("ERROR");

        if (settings.isGeneratingTT()) {
            final TripleTableLoader tt_loader =
                    new TripleTableLoader(settings, spark, statistics);
            tt_loader.load();

        }

        if (settings.isGeneratingWPT()) {
            statistics.setHasWPT(false);
            final WidePropertyTableLoader wptLoader = new WidePropertyTableLoader(settings, spark, statistics);
            wptLoader.load();
        }


        if (settings.isGeneratingVP()) {
            statistics.setHasVPTables(false);
            statistics.saveToFile(settings.getDatabaseName() + ".json");

            final VerticalPartitioningLoader vp_loader = new VerticalPartitioningLoader(settings, spark, statistics);
            vp_loader.load();

            statistics.setHasVPTables(true);
            statistics.setVpPartitionedBySubject(settings.isVpPartitionedBySubject());
            statistics.saveToFile(settings.getDatabaseName() + ".json");
        }


        if (settings.isGeneratingEXTVP()) {
            extvpSettings.loadUserSettings(args[0], "", args[1], 1.0F);

            String schemaType = args[2];

            if (schemaType.equals("EXTVP")) {
                System.out.println(args[2]);
                DataSetGenerator.generateDataSet("SS");
                DataSetGenerator.generateDataSet("OS");
                DataSetGenerator.generateDataSet("SO");
            }
        }




        if (settings.isComputingPropertyStatistics()) {
            assert statistics.hasVPTables() : "Not possible to compute property statistics. DB does not contain VP "
                    + "tables";

            statistics.computePropertyStatistics(spark);
            statistics.saveToFile(settings.getDatabaseName() + ".json");
        }


        /*
        * Store Schema in Different storage formats
        * */

        if (settings.isGeneratingTTCSV()) {
            StorageFormat s = new StorageFormat();
            String outputDir = args[0];

            File ttDirectory = new File(outputDir+"/CSV/tripletable.csv");

            if (! ttDirectory.exists()) {
                Dataset<Row> tt = spark.read().format("parquet").load(outputDir + "/" + "tripletable");
                s.toCSV(tt, outputDir + "/CSV/tripletable.csv");
            }
        }


        if (settings.isGeneratingTTORC()) {
            StorageFormat s = new StorageFormat();
            String outputDir = args[0];

            File ttDirectory = new File(outputDir+"/ORC/tripletable.orc");
            if (! ttDirectory.exists()) {
                Dataset<Row> tt = spark.read().format("parquet").load(outputDir + "/" + "tripletable");
                s.toORC(tt, outputDir + "/ORC/" + "tripletable.orc");
            }
        }


        if (settings.isGeneratingTTAvro()) {
            StorageFormat s = new StorageFormat();
            String outputDir = args[0];
            File ttDirectory = new File(outputDir+"/Avro/tripletable.avro");

            if (! ttDirectory.exists()) {
                Dataset<Row> tt = spark.read().format("parquet").load(outputDir + "/" + "tripletable");
                s.toAvro(tt, outputDir + "/Avro/" + "tripletable.avro");
            }
        }


        if (settings.isGeneratingWPTCSV()) {

            //TODO: We need to take care of the multi-valued predicates.

//            StorageFormat s = new StorageFormat();
//            String outputDir = args[0];
//            Dataset<Row> wpt = spark.read().format("parquet").load(outputDir + "/" + "wide_property_table");
//            s.toCSV(wpt, outputDir + "/CSV/" + "wide_property_table.csv");
        }


        if (settings.isGeneratingWPTORC()) {
            StorageFormat s = new StorageFormat();
            String outputDir = args[0];
            File wptDirectory = new File(outputDir+"/ORC/wide_property_table.orc");

            if (! wptDirectory.exists()) {
                Dataset<Row> wpt = spark.read().format("parquet").load(outputDir + "/" + "wide_property_table");
                s.toORC(wpt, outputDir + "/ORC/" + "wide_property_table.orc");
            }
        }


        if (settings.isGeneratingWPTAvro()) {
            StorageFormat s = new StorageFormat();
            String outputDir = args[0];
            File wptDirectory = new File(outputDir+"/Avro/wide_property_table.avro");

            if (! wptDirectory.exists()) {
                Dataset<Row> wpt = spark.read().format("parquet").load(outputDir + "/" + "wide_property_table");
                s.toAvro(wpt, outputDir + "/Avro/" + "wide_property_table.avro");
            }
        }




        if (settings.isGeneratingVPCSV()) {
            StorageFormat s = new StorageFormat();
            String outputDir = args[0];
            File vpDirectory = new File(outputDir+"/CSV/VP");

            if (! vpDirectory.exists()) {
                File[] files = new File(outputDir + "VP").listFiles();
                for (File f : files) {
                    Dataset<Row> vp = spark.read().format("parquet").load(outputDir + "/VP/" + f.getName());
                    s.toCSV(vp, outputDir + "/CSV/VP/" + f.getName() + ".csv");
                }
            }
        }


        if (settings.isGeneratingVPORC()) {
            StorageFormat s = new StorageFormat();
            String outputDir = args[0];
            File vpDirectory = new File(outputDir+"/ORC/VP");

            if (! vpDirectory.exists()) {
                File[] VPs = new File(outputDir + "VP").listFiles();

                for (File VP : VPs) {
                    Dataset<Row> vp = spark.read().format("parquet").load(outputDir + "/VP/" + VP.getName());
                    s.toORC(vp, outputDir + "/ORC/VP/" + VP.getName() + ".orc");
                }
            }
        }


        if (settings.isGeneratingVPAvro()) {
            StorageFormat s = new StorageFormat();
            String outputDir = args[0];
            File vpDirectory = new File(outputDir+"/Avro/VP");

            if (! vpDirectory.exists()) {
                File[] VPs = new File(outputDir + "VP").listFiles();
                for (File VP : VPs) {
                    Dataset<Row> vp = spark.read().format("parquet").load(outputDir + "/VP/" + VP.getName());
                    s.toAvro(vp, outputDir + "/Avro/VP/" + VP.getName() + ".avro");
                }
            }
        }


        if (settings.isGeneratingEXTVPCSV()) {
            StorageFormat s = new StorageFormat();
            String outputDir = args[0];
            File extvpDirectory = new File(outputDir+"/CSV/ExtVP");

            if (! extvpDirectory.exists()) {
                File[] EXTVPs = new File(outputDir + "EXTVP").listFiles();
                for (File Dir : EXTVPs) {

                    for (File Sub : Dir.listFiles()) {
                        for (File table : Sub.listFiles()) {
                            Dataset<Row> extvp = spark.read().format("parquet").load(table.getAbsolutePath());
                            s.toCSV(extvp, outputDir + "/CSV/EXTVP/" + Dir.getName() + "__" + Sub.getName() + "__" + table.getName() + ".csv");
                        }
                    }
                    System.out.println(Dir.getName() + " Has been converted into CSV!");
                }
            }
        }


        if (settings.isGeneratingEXTVPORC()) {
            StorageFormat s = new StorageFormat();
            String outputDir = args[0];
            File extvpDirectory = new File(outputDir+"/ORC/ExtVP");

            if (! extvpDirectory.exists()) {
                File[] EXTVPs = new File(outputDir + "EXTVP").listFiles();
                for (File Dir : EXTVPs) {

                    for (File Sub : Dir.listFiles()) {
                        for (File table : Sub.listFiles()) {
                            Dataset<Row> extvp = spark.read().format("parquet").load(table.getAbsolutePath());
                            s.toORC(extvp, outputDir + "/ORC/EXTVP/" + Dir.getName() + "__" + Sub.getName() + "__" + table.getName() + ".orc");
                        }
                    }

                    System.out.println(Dir.getName() + " Has been converted into ORC!");
                }
            }
        }


        if (settings.isGeneratingEXTVPAvro()) {
            StorageFormat s = new StorageFormat();
            String outputDir = args[0];
            File extvpDirectory = new File(outputDir+"/Avro/ExtVP");

            if (! extvpDirectory.exists()) {
                File[] EXTVPs = new File(outputDir + "EXTVP").listFiles();
                for (File Dir : EXTVPs) {

                    for (File Sub : Dir.listFiles()) {
                        for (File table : Sub.listFiles()) {
                            Dataset<Row> extvp = spark.read().format("parquet").load(table.getAbsolutePath());
                            s.toAvro(extvp, outputDir + "/Avro/EXTVP/" + Dir.getName() + "__" + Sub.getName() + "__" + table.getName() + ".avro");
                        }
                    }

                    System.out.println(Dir.getName() + " Has been converted into AVRO!");
                }
            }
        }


    }
}
