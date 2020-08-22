package net.navinet.research.spark.oncoemr;

import net.navinet.research.deidentification.PropertiesReader;
import net.navinet.research.deidentification.deidentify.transforms.DataFrameTransformFactory;
import net.navinet.research.deidentification.schema.FrameConfiguration;
import net.navinet.research.deidentification.schema.SchemaAccessor;
import net.navinet.research.deidentification.schema.TableSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.*;

/**
 * Driver class for de-identifiying CCLF Data
 */
public final class Driver {
  private static final Logger logger = LogManager.getLogger(Driver.class);
  static final String[] names = {
    "administrations", "allergy", "charge", "demographics", "diagnosis", "disease_status",
    "erx", "hospitalization", "insurance", "labs", "ob_gyn_history", "orders", "performance",
    "practice_location", "provider", "radiation_history", "radiology", "social_history", "staging",
    "surgical_history", "toxicities", "transfusion", "visit"
  };

  public static void main (String[] args) throws IOException{
    Map<String, String> cli = PropertiesReader.getCommandLineArgs(args);
    if (!cli.containsKey("input")) {
      throw new IllegalArgumentException("Incorrect usage; expected at least input=...");
    }


    // Sets configuration for the Spark job
    SparkConf conf = new SparkConf()
      .setAppName("OncoEMR File De-Identification"); // Name of the job
    JavaSparkContext sparkContext = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(sparkContext);

    DataFrameTransformFactory transformFactory = new DataFrameTransformFactory(sqlContext);
    SchemaAccessor schemaAccessor = new SchemaAccessor();
    List<FrameConfiguration> configs = Driver.getConfigElements(schemaAccessor);

    List<Path> paths = Driver.getCSVFilesInDirectory(new Path(cli.get("input")));

    //If not hdfs then add files to spark context
    if(!(cli.get("input").startsWith("hdfs"))) {
      for (Path oncoFile : paths) {
        sparkContext.addFile(oncoFile.toString());
      }
    }

    for (Path path : paths) {
      FrameConfiguration configuration = Driver.getMatchingFrameConfig(path, configs);
      TableSchema tableSchema = new TableSchema(configuration.columnStrategies(), transformFactory);

      DataFrame input = sqlContext.read()
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("nullValue", "")
        .option("mode", "PERMISSIVE")
        .option("path", path.toString())
        .load();
      DataFrame output = tableSchema.deidentify(input);
      output.show();
    }

    sparkContext.stop();
  }

  private static List<Path> getCSVFilesInDirectory(Path directory) throws IOException{

    try {
      List<Path> paths = new ArrayList<>();
      FileSystem fs = FileSystem.get(new Configuration());
      //TODO: apply filters...ends with csv
      RemoteIterator<LocatedFileStatus> filestatus = fs.listFiles(directory, false);
      while (filestatus.hasNext()) {
        paths.add(filestatus.next().getPath());
      }
      return  paths;
    } catch (IOException ex) {
      logger.error(ex);
      logger.error("Failed to list all files in given input directory to Spark context. Failing.");
      throw ex;
    }
  }

  private static FrameConfiguration getMatchingFrameConfig(Path file, List<FrameConfiguration> choices) {
    for (FrameConfiguration config : choices ) {
      if (file.getName().toString().matches(String.format(".*%s.*", config.name()))) {
        return config;
      }
    }

    String error = String.format("Could not find a configuration element for file %s",
      file.toString());

    logger.error(error);
    throw new IllegalStateException(error);
  }

  static List<FrameConfiguration> getConfigElements(SchemaAccessor schemaAccessor) {
    try (InputStream schema = Driver.class
      .getClassLoader()
      .getResourceAsStream("net/navinet/research/spark/oncoemr/oncoemr-schema.json");
    Reader schemaConfig = new InputStreamReader(schema)) {
      return schemaAccessor.readMultipleConfiguration(schemaConfig);
    } catch (IOException iex) {
      logger.error("Could not open schema configuration file", iex);
      throw new IllegalStateException("Failed to open schema configuration file", iex);
    }
  }
}
