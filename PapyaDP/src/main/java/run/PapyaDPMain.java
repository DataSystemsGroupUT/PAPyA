package run;

import com.google.common.io.Files;
import formatter.StorageFormat;
import loader.Settings;
import loader.TripleTableLoader;
import loader.VerticalPartitioningLoader;
import loader.WidePropertyTableLoader;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import loader.extvp.*;
import org.apache.spark.sql.SparkSession;
import partitioner.Partitioner;
import scala.collection.immutable.List;
import statistics.DatabaseStatistics;

import java.io.File;
import java.nio.file.StandardCopyOption;


public class PapyaDPMain {

    public static void main(String[] args) throws Exception {
        System.out.println("PAPyA DATA PREPARATOR...");

        final Settings settings = new Settings(args);
        final DatabaseStatistics statistics;
        statistics = new DatabaseStatistics(settings.getDatabaseName());

        final SparkSession spark = settings.loadSparkSession();

        spark.sparkContext().setLogLevel("ERROR");

        if (settings.isDroppingDB()) {
			spark.sql("DROP DATABASE IF EXISTS " + settings.getDatabaseName() + " CASCADE");
		}

        if (settings.isGeneratingTT()) {

            File ttDir = new File(args[0] + "/tripletable/");

            if (!ttDir.exists()) {
                final TripleTableLoader tt_loader = new TripleTableLoader(settings, spark, statistics);
                tt_loader.load();
                System.out.println("Triples Table Loaded (RAW).");
            }

        if (settings.isGeneratingTTCSV()) {

            StorageFormat s = new StorageFormat();
            Partitioner partitioner = new Partitioner();

            String outputDir = args[0];

            File ttDirectory = new File(outputDir + "/ST/");

                if (settings.isTtPartitionedHorizontally()) {
                    File ttHPDirectory = new File(ttDirectory + "/Horizontal/CSV/");

                    if (!ttHPDirectory.exists()) {

                        Dataset<Row> tt = spark.read().format("parquet").load(outputDir + "/" + "tripletable");

                        Dataset<Row> tthp = partitioner.partitionHorizontal(tt, 84);
                        s.toCSV(tthp, ttHPDirectory + "/tripletable");
                        System.out.println("Triples Table Loaded as CSV- Partitioned Horoizontally.");
                    }
                }

                if (settings.isTtPartitionedBySubject()) {
                    File ttSBPDirectory = new File(ttDirectory + "/Subject/CSV/");

                    if (!ttSBPDirectory.exists()) {

                        Dataset<Row> tt = spark.read().format("parquet").load(outputDir + "/" + "tripletable");

                        Dataset<Row> ttsbp = partitioner.partitionBySubject(tt, 84);
                        s.toCSV(ttsbp, ttSBPDirectory + "/tripletable");
                        System.out.println("Triples Table Loaded as CSV- Partitioned By Subject.");
                    }
                }

                if (settings.isTtPartitionedByPredicate()) {
                    File ttPBPDirectory = new File(ttDirectory + "/Predicate/CSV/");

                    if (!ttPBPDirectory.exists()) {

                        Dataset<Row> tt = spark.read().format("parquet").load(outputDir + "/" + "tripletable");

                        Dataset<Row> ttpbp = partitioner.partitionBySubject(tt, 84);
                        s.toCSV(ttpbp, ttPBPDirectory + "/tripletable");
                        System.out.println("Triples Table Loaded as CSV- Partitioned By Predicate.");
                    }
                }

                else {
                    File ttVanillaDirectory = new File(ttDirectory + "/VHDFS/CSV/");

                    if (!ttVanillaDirectory.exists()) {
                        Dataset<Row> tt = spark.read().format("parquet").load(outputDir + "/" + "tripletable");
                        s.toCSV(tt.repartition(1), ttVanillaDirectory + "/tripletable");
                        System.out.println("Triples Table Loaded as CSV- HDFS Partitioned.");
                    }
                }

        }
        if (settings.isGeneratingTTORC()) {

            StorageFormat s = new StorageFormat();
            Partitioner partitioner = new Partitioner();
            String outputDir = args[0];

            File ttDirectory = new File(outputDir + "/ST/");


            if (settings.isTtPartitionedHorizontally()) {
                File ttHPDirectory = new File(ttDirectory + "/Horizontal/ORC/");

                if (!ttHPDirectory.exists()) {

                    Dataset<Row> tt = spark.read().format("parquet").load(outputDir + "/" + "tripletable");

                    Dataset<Row> tthp = partitioner.partitionHorizontal(tt, 84);
                    s.toORC(tthp, ttHPDirectory + "/tripletable");
                    System.out.println("Triples Table Loaded as ORC- Partitioned Horizontally.");
                }
            }


            if (settings.isTtPartitionedByPredicate()) {
                File ttPBPDirectory = new File(ttDirectory + "/Predicate/ORC/");

                if (!ttPBPDirectory.exists()) {

                    Dataset<Row> tt = spark.read().format("parquet").load(outputDir + "/" + "tripletable");

                    Dataset<Row> ttpbp = partitioner.partitionBySubject(tt, 84);
                    s.toORC(ttpbp, ttPBPDirectory + "/tripletable");
                    System.out.println("Triples Table Loaded as ORC- Partitioned By Predicate.");
                }
            }

            if (settings.isTtPartitionedBySubject()) {
                File ttSBPDirectory = new File(ttDirectory + "/Subject/ORC/");

                if (!ttSBPDirectory.exists()) {

                    Dataset<Row> tt = spark.read().format("parquet").load(outputDir + "/" + "tripletable");

                    Dataset<Row> ttsbp = partitioner.partitionBySubject(tt, 84);
                    s.toORC(ttsbp, ttSBPDirectory + "/tripletable");
                    System.out.println("Triples Table Loaded as ORC- Partitioned By Subject.");

                }
            } else {
                File ttVanillaDirectory = new File(ttDirectory + "/VHDFS/ORC/");
                if (!ttVanillaDirectory.exists()) {
                    Dataset<Row> tt = spark.read().format("parquet").load(outputDir + "/" + "tripletable");
                    s.toORC(tt.repartition(1), ttVanillaDirectory + "/tripletable");
                    System.out.println("Triples Table Loaded as ORC- HDFS Partitioned.");
                }
            }

        }
        if (settings.isGeneratingTTAvro()) {

            StorageFormat s = new StorageFormat();
            Partitioner partitioner = new Partitioner();
            String outputDir = args[0];

            File ttDirectory = new File(outputDir + "/ST/");


            if (settings.isTtPartitionedHorizontally()) {
                File ttHPDirectory = new File(ttDirectory + "/Horizontal/Avro/");

                if (!ttHPDirectory.exists()) {

                    Dataset<Row> tt = spark.read().format("parquet").load(outputDir + "/" + "tripletable");

                    Dataset<Row> tthp = partitioner.partitionHorizontal(tt, 84);
                    s.toAvro(tthp, ttHPDirectory + "/tripletable");
                    System.out.println("Triples Table Loaded as AVRO- Partitioned Horizontally.");
                }
            }


            if (settings.isTtPartitionedByPredicate()) {
                File ttPBPDirectory = new File(ttDirectory + "/Predicate/Avro/");

                if (!ttPBPDirectory.exists()) {

                    Dataset<Row> tt = spark.read().format("parquet").load(outputDir + "/" + "tripletable");

                    Dataset<Row> ttpbp = partitioner.partitionBySubject(tt, 84);
                    s.toAvro(ttpbp, ttPBPDirectory + "/tripletable");
                    System.out.println("Triples Table Loaded as AVRO- Partitioned By Predicate.");
                }
            }

            if (settings.isTtPartitionedBySubject()) {
                File ttSBPDirectory = new File(ttDirectory + "/Subject/Avro/");

                if (!ttSBPDirectory.exists()) {

                    Dataset<Row> tt = spark.read().format("parquet").load(outputDir + "/" + "tripletable");

                    Dataset<Row> ttsbp = partitioner.partitionBySubject(tt, 84);
                    s.toAvro(ttsbp, ttSBPDirectory + "/tripletable");
                    System.out.println("Triples Table Loaded as Avro- Partitioned By Subject.");
                }
            } else {
                File ttVanillaDirectory = new File(ttDirectory + "/VHDFS/Avro/");
                if (!ttVanillaDirectory.exists()) {
                    Dataset<Row> tt = spark.read().format("parquet").load(outputDir + "/" + "tripletable");
                    s.toAvro(tt.repartition(1), ttVanillaDirectory + "/tripletable");
                     System.out.println("Triples Table Loaded as Avro- HDFS Partitioned.");
                }
            }

        }
        if (settings.isGeneratingTTParquet()) {

            StorageFormat s = new StorageFormat();
            Partitioner partitioner = new Partitioner();
            String outputDir = args[0];

            File ttDirectory = new File(outputDir + "/ST/");


            if (settings.isTtPartitionedHorizontally()) {
                File ttHPDirectory = new File(ttDirectory + "/Horizontal/Parquet/");

                if (!ttHPDirectory.exists()) {

                    Dataset<Row> tt = spark.read().format("parquet").load(outputDir + "/" + "tripletable");

                    Dataset<Row> tthp = partitioner.partitionHorizontal(tt, 84);
                    s.toParquet(tthp, ttHPDirectory + "/tripletable");
                     System.out.println("Triples Table Loaded as Parquet- Partitioned Horizontally.");
                }
            }


            if (settings.isTtPartitionedByPredicate()) {
                File ttPBPDirectory = new File(ttDirectory + "/Predicate/Parquet/");

                if (!ttPBPDirectory.exists()) {

                    Dataset<Row> tt = spark.read().format("parquet").load(outputDir + "/" + "tripletable");

                    Dataset<Row> ttpbp = partitioner.partitionBySubject(tt, 84);
                    s.toParquet(ttpbp, ttPBPDirectory + "/tripletable");
                     System.out.println("Triples Table Loaded as Parquet- Partitioned By Predicate.");
                }
            }

            if (settings.isTtPartitionedBySubject()) {
                File ttSBPDirectory = new File(ttDirectory + "/Subject/Parquet/");

                if (!ttSBPDirectory.exists()) {

                    Dataset<Row> tt = spark.read().format("parquet").load(outputDir + "/" + "tripletable");

                    Dataset<Row> ttsbp = partitioner.partitionBySubject(tt, 84);
                    s.toParquet(ttsbp, ttSBPDirectory + "/tripletable");
                    System.out.println("Triples Table Loaded as Parquet- Partitioned By Subject.");

                }
            } else {
                File ttVanillaDirectory = new File(ttDirectory + "/VHDFS/Parquet/");
                if (!ttVanillaDirectory.exists()) {
                    Dataset<Row> tt = spark.read().format("parquet").load(outputDir + "/" + "tripletable");
                    s.toParquet(tt.repartition(1), ttVanillaDirectory + "/tripletable");
                    System.out.println("Triples Table Loaded as Parquet- By HDFS Partitioned.");

                }
            }

        }

        }

        if (settings.isGeneratingWPT()) {
            statistics.setHasWPT(false);
            final WidePropertyTableLoader wptLoader = new WidePropertyTableLoader(settings, spark, statistics);
            wptLoader.load();


        if (settings.isGeneratingWPTCSV()) {
            //TODO: We need to take care of the multi-valued predicates.
        }

        if (settings.isGeneratingWPTORC()) {

            StorageFormat s = new StorageFormat();
            Partitioner partitioner = new Partitioner();
            String outputDir = args[0];

            File wptDirectory = new File(outputDir + "/WPT/");


            if (settings.isWptPartitionedHorizontally()) {
                File ttHPDirectory = new File(wptDirectory + "/Horizontal/ORC/");

                if (!ttHPDirectory.exists()) {

                    Dataset<Row> wpt = spark.read().format("parquet").load(outputDir + "/" + "wide_property_table");

                    Dataset<Row> wpthp = partitioner.partitionHorizontal(wpt, 84);
                    s.toORC(wpthp, ttHPDirectory + "/wide_property_table");
                    System.out.println("Wide Property Table Loaded as ORC- Partitioned Horizontally.");
                }
            }

            if (settings.isWptPartitionedBySubject()) {
                File wptSBPDirectory = new File(wptDirectory + "/Subject/ORC/");

                if (!wptSBPDirectory.exists()) {

                    Dataset<Row> wpt = spark.read().format("parquet").load(outputDir + "/" + "wide_property_table");

                    Dataset<Row> wptsbp = partitioner.partitionBySubject(wpt, 84);
                    s.toORC(wptsbp, wptSBPDirectory + "/wide_property_table");
                   System.out.println("Wide Property Table Loaded as ORC- Partitioned By Subject.");
                }
            } else {
                File wptVanillaDirectory = new File(wptDirectory + "/VHDFS/ORC/");

                if (!wptVanillaDirectory.exists()) {
                    Dataset<Row> wpt = spark.read().format("parquet").load(outputDir + "/" + "wide_property_table");
                    s.toORC(wpt.repartition(1), wptVanillaDirectory + "/wide_property_table");
                    System.out.println("Wide Property Table Loaded as ORC- HDFS Partitioned.");
                }
            }

        }
        if (settings.isGeneratingWPTAvro()) {

            StorageFormat s = new StorageFormat();
            Partitioner partitioner = new Partitioner();
            String outputDir = args[0];

            File wptDirectory = new File(outputDir + "/WPT/");


            if (settings.isWptPartitionedHorizontally()) {
                File ttHPDirectory = new File(wptDirectory + "/Horizontal/Avro/");

                if (!ttHPDirectory.exists()) {

                    Dataset<Row> wpt = spark.read().format("parquet").load(outputDir + "/" + "wide_property_table");

                    Dataset<Row> wpthp = partitioner.partitionHorizontal(wpt, 84);
                    s.toAvro(wpthp, ttHPDirectory + "/wide_property_table");
                    System.out.println("Wide Property Table Loaded as Avro- Partitioned Horizontally.");
                }
            }

            if (settings.isWptPartitionedBySubject()) {
                File wptSBPDirectory = new File(wptDirectory + "/Subject/Avro/");

                if (!wptSBPDirectory.exists()) {

                    Dataset<Row> wpt = spark.read().format("parquet").load(outputDir + "/" + "wide_property_table");

                    Dataset<Row> wptsbp = partitioner.partitionBySubject(wpt, 84);
                    s.toAvro(wptsbp, wptSBPDirectory + "/wide_property_table");
                    System.out.println("Wide Property Table Loaded as Avro- Partitioned By Subject.");
                }
            } else {
                File wptVanillaDirectory = new File(wptDirectory + "/VHDFS/Avro/");

                if (!wptVanillaDirectory.exists()) {
                    Dataset<Row> wpt = spark.read().format("parquet").load(outputDir + "/" + "wide_property_table");
                    s.toAvro(wpt.repartition(1), wptVanillaDirectory + "/wide_property_table");
                    System.out.println("Wide Property Table Loaded as Avro- HDFS Partitioned.");
                }
            }

        }
        if (settings.isGeneratingWPTParquet()) {

            StorageFormat s = new StorageFormat();
            Partitioner partitioner = new Partitioner();
            String outputDir = args[0];

            File wptDirectory = new File(outputDir + "/WPT/");


            if (settings.isWptPartitionedHorizontally()) {
                File ttHPDirectory = new File(wptDirectory + "/Horizontal/Parquet/");

                if (!ttHPDirectory.exists()) {

                    Dataset<Row> wpt = spark.read().format("parquet").load(outputDir + "/" + "wide_property_table");

                    Dataset<Row> wpthp = partitioner.partitionHorizontal(wpt, 84);
                    s.toParquet(wpthp, ttHPDirectory + "/wide_property_table");
                    System.out.println("Wide Property Table Loaded as Parquet- Partitioned Horizontally.");
                }
            }

            if (settings.isWptPartitionedBySubject()) {
                File wptSBPDirectory = new File(wptDirectory + "/Subject/Parquet/");

                if (!wptSBPDirectory.exists()) {

                    Dataset<Row> wpt = spark.read().format("parquet").load(outputDir + "/" + "wide_property_table");

                    Dataset<Row> wptsbp = partitioner.partitionBySubject(wpt, 84);
                    s.toParquet(wptsbp, wptSBPDirectory + "/wide_property_table");
                    System.out.println("Wide Property Table Loaded as Parquet- Partitioned By Subject.");
                }
            } else {
                File wptVanillaDirectory = new File(wptDirectory + "/VHDFS/Parquet/");

                if (!wptVanillaDirectory.exists()) {
                    Dataset<Row> wpt = spark.read().format("parquet").load(outputDir + "/" + "wide_property_table");
                    s.toParquet(wpt.repartition(1), wptVanillaDirectory + "/wide_property_table");
                    System.out.println("Wide Property Table Loaded as Parquet- HDFS Partitioned.");
                }
            }

        }

        }

        if (settings.isGeneratingVP()) {

            File vpDirectory = new File(args[0] + "/VP/");

            if (!vpDirectory.exists()) {
                statistics.setHasVPTables(false);
                statistics.saveToFile(settings.getDatabaseName() + ".json");

                final VerticalPartitioningLoader vp_loader = new VerticalPartitioningLoader(settings, spark, statistics);
                vp_loader.load();
                System.out.println("Vertical Tables Loaded (RAW).");

                statistics.setHasVPTables(true);
                statistics.setVpPartitionedBySubject(settings.isVpPartitionedBySubject());
                statistics.saveToFile(settings.getDatabaseName() + ".json");
            }


            if (settings.isGeneratingVPCSV()) {
                StorageFormat s = new StorageFormat();
                Partitioner partitioner = new Partitioner();
                String outputDir = args[0];

//            File vpDirectory = new File(outputDir + "/VP/");

             if (settings.isVpPartitionedHorizontally()) {
                File vpHPDirectory = new File(vpDirectory + "/Horizontal/CSV/");

                if (!vpHPDirectory.exists()) {
//                    File[] vpFiles = new File(vpDirectory.getPath()).listFiles();
                    File[] vpFiles=Helper.getListOfVPDirs(outputDir);
                    for (File vpFile : vpFiles) {
                        if ("Horizontal".equals(vpFile.getName()) || "Subject".equals(vpFile.getName()) || "VHDFS".equals(vpFile.getName())) {
                            continue;
                        } else {
                            Dataset<Row> vp = spark.read().format("parquet").load(outputDir + vpFile.getName());
                            Dataset<Row> vpHP = partitioner.partitionHorizontal(vp, 84);
                            s.toCSV(vpHP, vpHPDirectory + "/" + vpFile.getName());
                        }
                    }
                    System.out.println("Vertical Tables Loaded as CSV- Partitioned Horizontally.");
                }
            }

             if (settings.isVpPartitionedBySubject()) {
                File vpSBPDirectory = new File(vpDirectory + "/Subject/CSV/");

                if (!vpSBPDirectory.exists()) {
                    File[] vpFiles=Helper.getListOfVPDirs(outputDir);
                    for (File vpFile : vpFiles) {
                        if ("Horizontal".equals(vpFile.getName()) || "Subject".equals(vpFile.getName()) || "VHDFS".equals(vpFile.getName())) {
                            continue;
                        } else {
                            Dataset<Row> vp = spark.read().format("parquet").load(outputDir + vpFile.getName());
                            Dataset<Row> vpHP = partitioner.partitionBySubject(vp, 84);
                            s.toCSV(vpHP, vpSBPDirectory + "/" + vpFile.getName());
                        }

                    }
                    System.out.println("Vertical Tables Loaded as CSV- Partitioned By Subject.");
                }
            }
            else {
                File vpVanillaDirectory = new File(vpDirectory + "/VHDFS/CSV/");

                if (!vpVanillaDirectory.exists()) {
//                    File[] vpFiles = new File(vpDirectory.getPath()).listFiles();
                    File[] vpFiles=Helper.getListOfVPDirs(outputDir);
                    for (File vpFile : vpFiles) {
                        if ("Horizontal".equals(vpFile.getName()) || "Subject".equals(vpFile.getName()) || "VHDFS".equals(vpFile.getName())) {
                            continue;
                        } else {
                            Dataset<Row> vp = spark.read().format("parquet").load(outputDir + vpFile.getName());
                            s.toCSV(vp.repartition(1), vpVanillaDirectory + "/" + vpFile.getName());
                        }
                    }
                    System.out.println("Vertical Tables Loaded as CSV- HDFS Partitioned.");
                }
            }
        }
        if (settings.isGeneratingVPORC()) {
            StorageFormat s = new StorageFormat();
            Partitioner partitioner = new Partitioner();
            String outputDir = args[0];

            if (settings.isVpPartitionedHorizontally()) {
                File vpHPDirectory = new File(vpDirectory + "/Horizontal/ORC/");

                if (!vpHPDirectory.exists()) {
//                    File[] vpFiles = new File(vpDirectory.getPath()).listFiles();
                    File[] vpFiles=Helper.getListOfVPDirs(outputDir);

                    for (File vpFile : vpFiles) {
                        if ("Horizontal".equals(vpFile.getName()) || "Subject".equals(vpFile.getName()) || "VHDFS".equals(vpFile.getName())) {
                            continue;
                        } else {
                            Dataset<Row> vp = spark.read().format("parquet").load(outputDir + vpFile.getName());
                            Dataset<Row> vpHP = partitioner.partitionHorizontal(vp, 84);
                            s.toORC(vpHP, vpHPDirectory + "/" + vpFile.getName());
                        }
                    }
                    System.out.println("Vertical Tables Loaded as ORC- Partitioned Horizontally.");
                }
            }

            if (settings.isVpPartitionedBySubject()) {
                File vpSBPDirectory = new File(vpDirectory + "/Subject/ORC/");

                if (!vpSBPDirectory.exists()) {
//                    File[] vpFiles = new File(vpDirectory.getPath()).listFiles();
                    File[] vpFiles=Helper.getListOfVPDirs(outputDir);
                    for (File vpFile : vpFiles) {
                        if ("Horizontal".equals(vpFile.getName()) || "Subject".equals(vpFile.getName()) || "VHDFS".equals(vpFile.getName())) {
                            continue;
                        } else {
                            Dataset<Row> vp = spark.read().format("parquet").load(outputDir + vpFile.getName());
                            Dataset<Row> vpHP = partitioner.partitionBySubject(vp, 84);
                            s.toORC(vpHP, vpSBPDirectory + "/" + vpFile.getName());
                        }

                    }
                    System.out.println("Vertical Tables Loaded as ORC- Partitioned By Subject.");
                }
            }
            else {
                File vpVanillaDirectory = new File(vpDirectory + "/VHDFS/ORC/");

                if (!vpVanillaDirectory.exists()) {
//                    File[] vpFiles = new File(vpDirectory.getPath()).listFiles();
                    File[] vpFiles=Helper.getListOfVPDirs(outputDir);
                    for (File vpFile : vpFiles) {
                        if ("Horizontal".equals(vpFile.getName()) || "Subject".equals(vpFile.getName()) || "VHDFS".equals(vpFile.getName())) {
                            continue;
                        } else {
                            Dataset<Row> vp = spark.read().format("parquet").load(outputDir + vpFile.getName());
                            s.toORC(vp.repartition(1), vpVanillaDirectory + "/" + vpFile.getName());
                        }

                    }
                    System.out.println("Vertical Tables Loaded as ORC- HDFS Partitioned.");
                }
            }
        }
        if (settings.isGeneratingVPAvro()) {
            StorageFormat s = new StorageFormat();
            Partitioner partitioner = new Partitioner();
            String outputDir = args[0];

            if (settings.isVpPartitionedHorizontally()) {
                File vpHPDirectory = new File(vpDirectory + "/Horizontal/Avro/");

                if (!vpHPDirectory.exists()) {
//                    File[] vpFiles = new File(vpDirectory.getPath()).listFiles();
                      File[] vpFiles=Helper.getListOfVPDirs(outputDir);
                    for (File vpFile : vpFiles) {
                        if ("Horizontal".equals(vpFile.getName()) || "Subject".equals(vpFile.getName()) || "VHDFS".equals(vpFile.getName())) {
                            continue;
                        } else {
                            Dataset<Row> vp = spark.read().format("parquet").load(outputDir + vpFile.getName());
                            Dataset<Row> vpHP = partitioner.partitionHorizontal(vp, 84);
                            s.toAvro(vpHP, vpHPDirectory + "/" + vpFile.getName());
                        }
                    }
                    System.out.println("Vertical Tables Loaded as Avro- Partitioned Horizontally.");
                }
            }

            if (settings.isVpPartitionedBySubject()) {
                File vpSBPDirectory = new File(vpDirectory + "/Subject/Avro/");

                if (!vpSBPDirectory.exists()) {
//                    File[] vpFiles = new File(vpDirectory.getPath()).listFiles();
                    File[] vpFiles=Helper.getListOfVPDirs(outputDir);
                    for (File vpFile : vpFiles) {
                        if ("Horizontal".equals(vpFile.getName()) || "Subject".equals(vpFile.getName()) || "VHDFS".equals(vpFile.getName())) {
                            continue;
                        } else {
                            Dataset<Row> vp = spark.read().format("parquet").load(outputDir  + vpFile.getName());
                            Dataset<Row> vpHP = partitioner.partitionBySubject(vp, 84);
                            s.toAvro(vpHP, vpSBPDirectory + "/" + vpFile.getName());
                        }

                    }
                    System.out.println("Vertical Tables Loaded as Avro- Partitioned By Subject.");
                }
            }
            else {
                File vpVanillaDirectory = new File(vpDirectory + "/VHDFS/Avro/");

                if (!vpVanillaDirectory.exists()) {
//                    File[] vpFiles = new File(vpDirectory.getPath()).listFiles();
                      File[] vpFiles=Helper.getListOfVPDirs(outputDir);
                    for (File vpFile : vpFiles) {
                        if ("Horizontal".equals(vpFile.getName()) || "Subject".equals(vpFile.getName()) || "VHDFS".equals(vpFile.getName())) {
                            continue;
                        } else {
                            Dataset<Row> vp = spark.read().format("parquet").load(outputDir  + vpFile.getName());
                            s.toAvro(vp.repartition(1), vpVanillaDirectory + "/" + vpFile.getName());
                        }

                    }
                    System.out.println("Vertical Tables Loaded as Avro- HDFS Partitioned.");
                }
            }
        }
        if (settings.isGeneratingVPParquet()) {
            StorageFormat s = new StorageFormat();
            Partitioner partitioner = new Partitioner();
            String outputDir = args[0];

            if (settings.isVpPartitionedHorizontally()) {
                File vpHPDirectory = new File(vpDirectory + "/Horizontal/Parquet/");

                if (!vpHPDirectory.exists()) {
//                    File[] vpFiles = new File(vpDirectory.getPath()).listFiles();

                    File[] vpFiles=Helper.getListOfVPDirs(outputDir);
                    for (File vpFile : vpFiles) {
                        if ("Horizontal".equals(vpFile.getName()) || "Subject".equals(vpFile.getName()) || "VHDFS".equals(vpFile.getName())) {
                            continue;
                        } else {
                            Dataset<Row> vp = spark.read().format("parquet").load(outputDir + vpFile.getName());
                            Dataset<Row> vpHP = partitioner.partitionHorizontal(vp, 84);
                            s.toParquet(vpHP, vpHPDirectory + "/" + vpFile.getName());
                        }
                    }
                    System.out.println("Vertical Tables Loaded as Parquet- Partitioned Horizontally.");
                }
            }

            if (settings.isVpPartitionedBySubject()) {
                File vpSBPDirectory = new File(vpDirectory + "/Subject/Parquet/");

                if (!vpSBPDirectory.exists()) {
//                    File[] vpFiles = new File(vpDirectory.getPath()).listFiles();
                    File[] vpFiles=Helper.getListOfVPDirs(outputDir);
                    for (File vpFile : vpFiles) {
                        if ("Horizontal".equals(vpFile.getName()) || "Subject".equals(vpFile.getName()) || "VHDFS".equals(vpFile.getName())) {
                            continue;
                        } else {
                            Dataset<Row> vp = spark.read().format("parquet").load(outputDir  + vpFile.getName());
                            Dataset<Row> vpHP = partitioner.partitionBySubject(vp, 84);
                            s.toParquet(vpHP, vpSBPDirectory + "/" + vpFile.getName());
                        }
                    }
                    System.out.println("Vertical Tables Loaded as Parquet- Partitioned By Subject.");
                }
            }
            else {
                File vpVanillaDirectory = new File(vpDirectory + "/VHDFS/Parquet/");

                if (!vpVanillaDirectory.exists()) {
//                    File[] vpFiles = new File(vpDirectory.getPath()).listFiles();
                    File[] vpFiles=Helper.getListOfVPDirs(outputDir);
                    for (File vpFile : vpFiles) {
                        if ("Horizontal".equals(vpFile.getName()) || "Subject".equals(vpFile.getName()) || "VHDFS".equals(vpFile.getName())) {
                            continue;
                        } else {
                            Dataset<Row> vp = spark.read().format("parquet").load(outputDir  + vpFile.getName());
                            s.toParquet(vp.repartition(1), vpVanillaDirectory + "/" + vpFile.getName());
                        }

                    }
                    System.out.println("Vertical Tables Loaded as Parquet- HDFS Partitioned.");
                }
            }
        }


        }

        if (settings.isGeneratingEXTVP()) {
            extvpSettings.loadUserSettings(args[0], 1.0F);

            File extvpDirectory = new File(args[0] + "/ExtVP/");

            if (!extvpDirectory.exists()) {

                String schemaType = "EXTVP";

                if (schemaType.equals("EXTVP")) {
                    System.out.println(args[2]);
                    DataSetGenerator.generateDataSet("SS");
                    DataSetGenerator.generateDataSet("OS");
                    DataSetGenerator.generateDataSet("SO");
                }
            }

          /**
         * ExtVP
         */
        if (settings.isGeneratingEXTVPORC()) {

            StorageFormat s = new StorageFormat();
            Partitioner partitioner=new Partitioner();
            String outputDir = args[0];

//            File extvpDirectory = new File(outputDir + "/ExtVP/");

            if (settings.isExtvpPartitionedBySubject()) {
                File extVpSBPDirectory = new File(extvpDirectory + "/Subject/ORC/");

                if (!extVpSBPDirectory.exists()) {
                    File[] EXTVPs = new File(extvpDirectory.getPath()).listFiles();
                        for (File Dir : EXTVPs) {
                             if ("Horizontal".equals(Dir.getName()) || "Subject".equals(Dir.getName()) || "VHDFS".equals(Dir.getName())) {
                                 continue;
                             }
                             else {
                                 for (File Sub : Dir.listFiles()) {
                                     for (File table : Sub.listFiles()) {
                                         Dataset<Row> extvp = spark.read().format("parquet").load(table.getAbsolutePath());
                                         Dataset<Row> extvpSBP = partitioner.partitionBySubject(extvp, 84);
                                         s.toORC(extvpSBP, extVpSBPDirectory + "/" + Dir.getName() + "__" + Sub.getName() + "__" + table.getName());
                                     }
                                 }
                             }
                        System.out.println(Dir.getName() + " Has been converted into ORC-Partitioned By Subject!");
                    }
                }

            }
            if (settings.isExtvpPartitionedHorizontally()) {
                    File extVpHPDirectory = new File(extvpDirectory + "/Horizontal/ORC/");

                    if (!extVpHPDirectory.exists()) {
                        File[] EXTVPs = new File(extvpDirectory.getPath()).listFiles();

                        for (File Dir : EXTVPs) {
                             if ("Horizontal".equals(Dir.getName()) || "Subject".equals(Dir.getName()) || "VHDFS".equals(Dir.getName())) {
                                 continue;
                             }
                             else {
                                 for (File Sub : Dir.listFiles()) {
                                     for (File table : Sub.listFiles()) {
                                         Dataset<Row> extvp = spark.read().format("parquet").load(table.getAbsolutePath());
                                         Dataset<Row> extvpSBP = partitioner.partitionHorizontal(extvp, 84);
                                         s.toORC(extvpSBP, extVpHPDirectory + "/" + Dir.getName() + "__" + Sub.getName() + "__" + table.getName());
                                     }
                                 }
                             }
                            System.out.println(Dir.getName() + " Has been converted into ORC-Partitioned Horizontally!");
                        }
                    }
                }


             else{
                 File extVpVHDFSDirectory = new File(extvpDirectory + "/VHDFS/ORC/");

                 if (!extVpVHDFSDirectory.exists()) {
                    File[] EXTVPs = new File(outputDir + "EXTVP").listFiles();
                    for (File Dir : EXTVPs) {
                             if ("Horizontal".equals(Dir.getName()) || "Subject".equals(Dir.getName()) || "VHDFS".equals(Dir.getName())) {
                                 continue;
                             }
                             else {
                                 for (File Sub : Dir.listFiles()) {
                                     for (File table : Sub.listFiles()) {
                                         Dataset<Row> extvp = spark.read().format("parquet").load(table.getAbsolutePath());
                                         s.toORC(extvp.repartition(1), extVpVHDFSDirectory + "/" + Dir.getName() + "__" + Sub.getName() + "__" + table.getName() + ".csv");
                                     }
                                 }
                             }
                        System.out.println(Dir.getName() + " Has been converted into ORC-VHDFS!");
                    }
                }
             }

        }
        if (settings.isGeneratingEXTVPCSV()) {

            StorageFormat s = new StorageFormat();
            Partitioner partitioner=new Partitioner();
            String outputDir = args[0];

//            File extvpDirectory = new File(outputDir + "/ExtVP/");

            if (settings.isExtvpPartitionedBySubject()) {
                File extVpSBPDirectory = new File(extvpDirectory + "/Subject/CSV/");

                if (!extVpSBPDirectory.exists()) {
                    File[] EXTVPs = new File(extvpDirectory.getPath()).listFiles();
                        for (File Dir : EXTVPs) {
                             if ("Horizontal".equals(Dir.getName()) || "Subject".equals(Dir.getName()) || "VHDFS".equals(Dir.getName())) {
                                 continue;
                             }
                             else {
                                 for (File Sub : Dir.listFiles()) {
                                     for (File table : Sub.listFiles()) {
                                         Dataset<Row> extvp = spark.read().format("parquet").load(table.getAbsolutePath());
                                         Dataset<Row> extvpSBP = partitioner.partitionBySubject(extvp, 84);
                                         s.toCSV(extvpSBP, extVpSBPDirectory + "/" + Dir.getName() + "__" + Sub.getName() + "__" + table.getName());
                                     }
                                 }
                             }
                        System.out.println(Dir.getName() + " Has been converted into CSV-Partitioned By Subject!");
                    }
                }

            }
            if (settings.isExtvpPartitionedHorizontally()) {
                    File extVpHPDirectory = new File(extvpDirectory + "/Horizontal/CSV/");

                    if (!extVpHPDirectory.exists()) {
                        File[] EXTVPs = new File(extvpDirectory.getPath()).listFiles();

                        for (File Dir : EXTVPs) {
                             if ("Horizontal".equals(Dir.getName()) || "Subject".equals(Dir.getName()) || "VHDFS".equals(Dir.getName())) {
                                 continue;
                             }
                             else {
                                 for (File Sub : Dir.listFiles()) {
                                     for (File table : Sub.listFiles()) {
                                         Dataset<Row> extvp = spark.read().format("parquet").load(table.getAbsolutePath());
                                         Dataset<Row> extvpSBP = partitioner.partitionHorizontal(extvp, 84);
                                         s.toCSV(extvpSBP, extVpHPDirectory + "/" + Dir.getName() + "__" + Sub.getName() + "__" + table.getName());
                                     }
                                 }
                             }
                            System.out.println(Dir.getName() + " Has been converted into CSV-Partitioned Horizontally!");
                        }
                    }
                }


             else{
                 File extVpVHDFSDirectory = new File(extvpDirectory + "/VHDFS/CSV/");

                 if (!extVpVHDFSDirectory.exists()) {
                    File[] EXTVPs = new File(outputDir + "EXTVP").listFiles();
                    for (File Dir : EXTVPs) {
                             if ("Horizontal".equals(Dir.getName()) || "Subject".equals(Dir.getName()) || "VHDFS".equals(Dir.getName())) {
                                 continue;
                             }
                             else {
                                 for (File Sub : Dir.listFiles()) {
                                     for (File table : Sub.listFiles()) {
                                         Dataset<Row> extvp = spark.read().format("parquet").load(table.getAbsolutePath());
                                         s.toCSV(extvp.repartition(1), extVpVHDFSDirectory + "/" + Dir.getName() + "__" + Sub.getName() + "__" + table.getName() + ".csv");
                                     }
                                 }
                             }
                        System.out.println(Dir.getName() + " Has been converted into CSV-VHDFS!");
                    }
                }
             }

        }
        if (settings.isGeneratingEXTVPAvro()) {

            StorageFormat s = new StorageFormat();
            Partitioner partitioner=new Partitioner();
            String outputDir = args[0];

//            File extvpDirectory = new File(outputDir + "/ExtVP/");

            if (settings.isExtvpPartitionedBySubject()) {
                File extVpSBPDirectory = new File(extvpDirectory + "/Subject/Avro/");

                if (!extVpSBPDirectory.exists()) {
                    File[] EXTVPs = new File(extvpDirectory.getPath()).listFiles();
                        for (File Dir : EXTVPs) {
                             if ("Horizontal".equals(Dir.getName()) || "Subject".equals(Dir.getName()) || "VHDFS".equals(Dir.getName())) {
                                 continue;
                             }
                             else {
                                 for (File Sub : Dir.listFiles()) {
                                     for (File table : Sub.listFiles()) {
                                         Dataset<Row> extvp = spark.read().format("parquet").load(table.getAbsolutePath());
                                         Dataset<Row> extvpSBP = partitioner.partitionBySubject(extvp, 84);
                                         s.toAvro(extvpSBP, extVpSBPDirectory + "/" + Dir.getName() + "__" + Sub.getName() + "__" + table.getName());
                                     }
                                 }
                             }
                        System.out.println(Dir.getName() + " Has been converted into Avro-Partitioned By Subject!");
                    }
                }

            }
            if (settings.isExtvpPartitionedHorizontally()) {
                    File extVpHPDirectory = new File(extvpDirectory + "/Horizontal/Avro/");

                    if (!extVpHPDirectory.exists()) {
                        File[] EXTVPs = new File(extvpDirectory.getPath()).listFiles();

                        for (File Dir : EXTVPs) {
                             if ("Horizontal".equals(Dir.getName()) || "Subject".equals(Dir.getName()) || "VHDFS".equals(Dir.getName())) {
                                 continue;
                             }
                             else {
                                 for (File Sub : Dir.listFiles()) {
                                     for (File table : Sub.listFiles()) {
                                         Dataset<Row> extvp = spark.read().format("parquet").load(table.getAbsolutePath());
                                         Dataset<Row> extvpSBP = partitioner.partitionHorizontal(extvp, 84);
                                         s.toAvro(extvpSBP, extVpHPDirectory + "/" + Dir.getName() + "__" + Sub.getName() + "__" + table.getName());
                                     }
                                 }
                             }
                            System.out.println(Dir.getName() + " Has been converted into Avro-Partitioned Horizontally!");
                        }
                    }
                }


             else{
                 File extVpVHDFSDirectory = new File(extvpDirectory + "/VHDFS/Avro/");

                 if (!extVpVHDFSDirectory.exists()) {
                    File[] EXTVPs = new File(outputDir + "EXTVP").listFiles();
                    for (File Dir : EXTVPs) {
                             if ("Horizontal".equals(Dir.getName()) || "Subject".equals(Dir.getName()) || "VHDFS".equals(Dir.getName())) {
                                 continue;
                             }
                             else {
                                 for (File Sub : Dir.listFiles()) {
                                     for (File table : Sub.listFiles()) {
                                         Dataset<Row> extvp = spark.read().format("parquet").load(table.getAbsolutePath());
                                         s.toAvro(extvp.repartition(1), extVpVHDFSDirectory + "/" + Dir.getName() + "__" + Sub.getName() + "__" + table.getName() + ".csv");
                                     }
                                 }
                             }
                        System.out.println(Dir.getName() + " Has been converted into Avro-VHDFS!");
                    }
                }
             }

        }
        if (settings.isGeneratingEXTVPParquet()) {

            StorageFormat s = new StorageFormat();
            Partitioner partitioner=new Partitioner();
            String outputDir = args[0];

//            File extvpDirectory = new File(outputDir + "/ExtVP/");

            if (settings.isExtvpPartitionedBySubject()) {
                File extVpSBPDirectory = new File(extvpDirectory + "/Subject/Parquet/");

                if (!extVpSBPDirectory.exists()) {
                    File[] EXTVPs = new File(extvpDirectory.getPath()).listFiles();
                        for (File Dir : EXTVPs) {
                             if ("Horizontal".equals(Dir.getName()) || "Subject".equals(Dir.getName()) || "VHDFS".equals(Dir.getName())) {
                                 continue;
                             }
                             else {
                                 for (File Sub : Dir.listFiles()) {
                                     for (File table : Sub.listFiles()) {
                                         Dataset<Row> extvp = spark.read().format("parquet").load(table.getAbsolutePath());
                                         Dataset<Row> extvpSBP = partitioner.partitionBySubject(extvp, 84);
                                         s.toParquet(extvpSBP, extVpSBPDirectory + "/" + Dir.getName() + "__" + Sub.getName() + "__" + table.getName());
                                     }
                                 }
                             }
                        System.out.println(Dir.getName() + " Has been converted into Parquet-Partitioned By Subject!");
                    }
                }

            }
            if (settings.isExtvpPartitionedHorizontally()) {
                    File extVpHPDirectory = new File(extvpDirectory + "/Horizontal/Parquet/");

                    if (!extVpHPDirectory.exists()) {
                        File[] EXTVPs = new File(extvpDirectory.getPath()).listFiles();

                        for (File Dir : EXTVPs) {
                             if ("Horizontal".equals(Dir.getName()) || "Subject".equals(Dir.getName()) || "VHDFS".equals(Dir.getName())) {
                                 continue;
                             }
                             else {
                                 for (File Sub : Dir.listFiles()) {
                                     for (File table : Sub.listFiles()) {
                                         Dataset<Row> extvp = spark.read().format("parquet").load(table.getAbsolutePath());
                                         Dataset<Row> extvpSBP = partitioner.partitionHorizontal(extvp, 84);
                                         s.toParquet(extvpSBP, extVpHPDirectory + "/" + Dir.getName() + "__" + Sub.getName() + "__" + table.getName());
                                     }
                                 }
                             }
                            System.out.println(Dir.getName() + " Has been converted into Parquet-Partitioned Horizontally!");
                        }
                    }
                }


             else{
                 File extVpVHDFSDirectory = new File(extvpDirectory + "/VHDFS/Parquet/");

                 if (!extVpVHDFSDirectory.exists()) {
                    File[] EXTVPs = new File(outputDir + "EXTVP").listFiles();
                    for (File Dir : EXTVPs) {
                             if ("Horizontal".equals(Dir.getName()) || "Subject".equals(Dir.getName()) || "VHDFS".equals(Dir.getName())) {
                                 continue;
                             }
                             else {
                                 for (File Sub : Dir.listFiles()) {
                                     for (File table : Sub.listFiles()) {
                                         Dataset<Row> extvp = spark.read().format("parquet").load(table.getAbsolutePath());
                                         s.toParquet(extvp.repartition(1), extVpVHDFSDirectory + "/" + Dir.getName() + "__" + Sub.getName() + "__" + table.getName() + ".csv");
                                     }
                                 }
                             }
                        System.out.println(Dir.getName() + " Has been converted into Parquet-VHDFS!");
                    }
                }
             }

        }


        }

        if (settings.isComputingPropertyStatistics()) {
            assert statistics.hasVPTables() : "Not possible to compute property statistics. DB does not contain VP "
                    + "tables";

            statistics.computePropertyStatistics(spark);
            statistics.saveToFile(settings.getDatabaseName() + ".json");
        }

    }
}
