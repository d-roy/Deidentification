package net.navinet.research.spark.cclf;

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
import org.apache.spark.sql.SaveMode;


/**
 * Driver class for de-identifiying CCLF Data
 */
public class Driver {
    private static final Logger logger = LogManager.getLogger(Driver.class);


    public static void main(String[] args) throws  IOException {
        Map<String, String> cli = PropertiesReader.getCommandLineArgs(args);
        if (!cli.containsKey("input")) {
            throw new IllegalArgumentException("Command line arguments must contain "
                    + "at least \"input=...\"");
        }

        // Sets configuration for the Spark job
        SparkConf conf = new SparkConf()
                .setAppName("CCLF File De-Identification"); // Name of the job
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sparkContext);

        DataFrameTransformFactory transformFactory = new DataFrameTransformFactory(sqlContext);
        SchemaAccessor schemaAccessor = new SchemaAccessor();
        List<FrameConfiguration> configs = Driver.getConfigElements(schemaAccessor);

        List<Path> paths = Driver.getCCLFFilesInDirectory(new Path(cli.get("input")));

        //If not hdfs then add files to spark context
        if(!(cli.get("input").startsWith("hdfs"))) {
            for (Path cclfFile : paths) {
                sparkContext.addFile(cclfFile.toString());
            }
        }

        for (Path cclfFile : paths) {
            FrameConfiguration configuration = Driver.getMatchingFrameConfig(cclfFile, configs);
            TableSchema tableSchema = new TableSchema(configuration.columnStrategies(), transformFactory);

            DataFrame input = sqlContext.read()
                    .format("cclf")
                    .option("path", cclfFile.toString())
                    .option("schema_path", getSchemaPathFromFileName(cclfFile.toString()))
                    .load();


            DataFrame output = tableSchema.deidentify(input);
            output.show();

            /*
            //save file back to cclf
            output.write()
                .mode(SaveMode.Overwrite)
                .format("cclf")
                .option("schema_path", getSchemaPathFromFileName(cclfFile.toString()))
                .option("path", cclfFile.toString()+"_")
                .save(cclfFile.toString()+"_");
            */
            /*
            //Save file as csv file
            Map<String, String> options =  new HashMap<>();
            options.put("inferSchema", "true");
            options.put("header", "true");
            //options.put("codec", "org.apache.hadoop.io.compress.GzipCodec");
            output.write()
                .format("com.databricks.spark.csv")
                .options(options)
                .save(configuration.name() + ".csv");
            */
        }
        sparkContext.stop();
    }

    private static List<Path> getCCLFFilesInDirectory(Path directory) throws IOException {
        try {
            List<Path> paths = new ArrayList<>();
            FileSystem fs = FileSystem.get(new Configuration());
            //TODO: apply filters
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

  static FrameConfiguration getMatchingFrameConfig(Path file, List<FrameConfiguration> choices) {
    for (FrameConfiguration config : choices) {
      if (file.getName().toString().matches(String.format(".*%s.*", config.name()))) {
        return config;
      }
    }
    String error = String.format(
      "Could not find a configuration element for file %s", file.toString());
    logger.error(error);
    throw new IllegalStateException(error);
  }

  static List<FrameConfiguration> getConfigElements(SchemaAccessor schemaAccessor) {
    try (InputStream schema = Driver.class
      .getClassLoader().getResourceAsStream("net/navinet/research/spark/cclf/cclf-schema.json");
         Reader schemaConfig = new InputStreamReader(schema)) {
      return schemaAccessor.readMultipleConfiguration(schemaConfig);
    } catch (IOException iex) {
      logger.error("Could not open schema configuration file", iex);
      throw new IllegalStateException("Failed to open schema config file", iex);
    }
  }

  /**
   * @param fileName Name of CCLF file to be read
   * @return Name of schema file associated with the given CCLF file
   * @throws IllegalArgumentException if associated schema is not found
   */
  static String getSchemaPathFromFileName(String fileName) {
    if (fileName.toUpperCase().contains("CCLF1")) {
      return "net/navinet/research/spark/cclf/claims_header_file_schema.json";
    } else if (fileName.toUpperCase().contains("CCLF2")) {
      return "net/navinet/research/spark/cclf/claims_revenue_file_schema.json";
    } else if (fileName.toUpperCase().contains("CCLF3")) {
      return "net/navinet/research/spark/cclf/procedure_file_schema.json";
    } else if (fileName.toUpperCase().contains("CCLF4")) {
      return "net/navinet/research/spark/cclf/diagnosis_file_schema.json";
    } else if (fileName.toUpperCase().contains("CCLF5")) {
      return "net/navinet/research/spark/cclf/part_b_physician_file_schema.json";
    } else if (fileName.toUpperCase().contains("CCLF6")) {
      return "net/navinet/research/spark/cclf/part_b_dme_file_schema.json";
    } else if (fileName.toUpperCase().contains("CCLF7")) {
      return "net/navinet/research/spark/cclf/part_d_file_schema.json";
    } else if (fileName.toUpperCase().contains("CCLF8")) {
      return "net/navinet/research/spark/cclf/beneficiary_demographics_file_schema.json";
    } else if (fileName.toUpperCase().contains("CCLF9")) {
      return "net/navinet/research/spark/cclf/beneficiary_xref_file_schema.json";
    } else {
      logger.error("No schema found for file " + fileName);
      throw new IllegalArgumentException("No schema for file " + fileName);
    }
  }
}
