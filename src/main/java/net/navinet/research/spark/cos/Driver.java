package net.navinet.research.spark.cos;

import net.navinet.research.deidentification.PropertiesReader;
import net.navinet.research.deidentification.connection.ConnectionFactory;
import net.navinet.research.deidentification.connection.JDBCConnection;
import net.navinet.research.deidentification.deidentify.transforms.DataFrameTransformFactory;
import net.navinet.research.deidentification.schema.FrameConfiguration;
import net.navinet.research.deidentification.schema.SchemaAccessor;
import net.navinet.research.deidentification.schema.TableSchema;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Driver class for de-identifying structured data through Spark.
 */
public final class Driver {
  private static final Logger logger = Logger.getLogger(Driver.class);

  /**
   * Start of the program. Uses the applications.properties file to retrieve data and how to
   * transform it in order to de-identify protected health information.
   *
   * It expects the following command line arguments :
   * <ol>
   *   <li>Source configuration format</li>
   *   <li>Schema configuration format</li>
   *   <li>Output configuration format</li>
   * </ol>
   *
   *
   *
   * @param args The command line arguments
   */
  public static void main(String[] args) {
    Map<String, String> cli = PropertiesReader.getCommandLineArgs(args);
    if (!cli.containsKey("input") || !cli.containsKey("schema") || !cli.containsKey("output")) {
      throw new IllegalArgumentException("Incorrect usage; must be called with at least "
        + "input=... schema=... output=...");
    }



    // Sets configuration for the Spark job
    SparkConf conf = new SparkConf()
      .setAppName("Deidentification Spark"); // Name of the job
    // SparkContext working over Java collections
    JavaSparkContext sparkContext = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(sparkContext);

    sparkContext.addFile(cli.get("input"));
    sparkContext.addFile(cli.get("schema"));

    try (
      FileReader sourceConfiguration = new FileReader(cli.get("input"));
      FileReader schemaConfiguration = new FileReader(cli.get("schema"))
    ) {
      ConnectionFactory connectionFactory = new ConnectionFactory(sqlContext);
      DataFrameTransformFactory transformFactory = new DataFrameTransformFactory(sqlContext);
      JDBCConnection sourceConnection = connectionFactory.fromConfiguration(sourceConfiguration);

      SchemaAccessor accessor = new SchemaAccessor();

      List<FrameConfiguration> configurations = accessor
        .readMultipleConfiguration(schemaConfiguration);

      for (FrameConfiguration config : configurations) {

        DataFrame input = sourceConnection.read(config.name(), config.partitioningColumn());

        TableSchema tableSchema = new TableSchema(config.columnStrategies(), transformFactory);


        DataFrame output = tableSchema.deidentify(input);


        //output.persist(StorageLevel.MEMORY_ONLY_SER());
        //System.out.println("Count: " + output.count());
        //output.registerTempTable(config.name());
        output.show();
      }
    } catch (IOException ioex) {
      logger.error("Failed to open configuration file", ioex);
    } finally {
      sparkContext.stop();
    }
  }
}