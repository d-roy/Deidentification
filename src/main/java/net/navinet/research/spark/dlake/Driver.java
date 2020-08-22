package net.navinet.research.spark.dlake;

import net.navinet.research.deidentification.PropertiesReader;
import net.navinet.research.deidentification.deidentify.transforms.DataFrameTransformFactory;
import net.navinet.research.deidentification.schema.FrameConfiguration;
import net.navinet.research.deidentification.schema.SchemaAccessor;
import net.navinet.research.deidentification.schema.TableSchema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Driver class for de-identifiying Onco EMR Data:
 */
public final class Driver {
  private static final Logger logger = LogManager.getLogger(Driver.class);

  public static void main(String[] args) throws IOException {
    String input_path = "", output_path = "";
    String realm_type = "";
    String db_schema = "";
    String schema_path = "";
    String jdbc_in_conn = "", jdbc_out_conn = "";
    String jdbc_in_url = "", jdbc_out_url = "";
    String jdbc_in_schema = "", jdbc_out_schema = "";
    String jdbc_in_user = "", jdbc_in_password = "";
    String jdbc_in_driver = "";
    Properties in_prop, out_prop = null;
    Map<String, String> cli = PropertiesReader.getCommandLineArgs(args);

    if (cli.containsKey("realm_type")) {
      realm_type = cli.get("realm_type");
      System.out.println("realm_type => " + "("+realm_type+")");
          if (!realm_type.equals("local") && !realm_type.equals("hdfs") && !realm_type.equals
              ("jdbc") && !realm_type.equals("jdbc-local")) {
            throw new IllegalArgumentException("Incorrect usage; expected " +
                "realm_type=[local|hdfs|jdbc|jdbc-local]...");
          }
    } else {
      realm_type = "hdfs";
    }

    if (cli.containsKey("input") && cli.containsKey("jdbc_input")){
      throw new IllegalArgumentException("Incorrect usage; expected input=... or jdbc_input=...");
    } else if (cli.containsKey("input")) {
      input_path = cli.get("input");
    } else if (cli.containsKey("jdbc_input")) {
      jdbc_in_conn = cli.get("jdbc_input");
    } else {
      throw new IllegalArgumentException("Incorrect usage; expected input=... or jdbc_input=...");
    }

    if (cli.containsKey("db_schema") && cli.containsKey("output")) {
      throw new IllegalArgumentException("Incorrect usage; db_schema and output are mutually " +
          "exclusive...");
    } else if (cli.containsKey("db_schema") && cli.containsKey("jdbc_output")) {
      throw new IllegalArgumentException("Incorrect usage; db_schema and jdbc_output are mutually" +
          " exclusive...");
    } else if (cli.containsKey("jdbc_output") && cli.containsKey("output")) {
      throw new IllegalArgumentException("Incorrect usage; output and jdbc_output are mutually" +
          " exclusive...");
    } else if (cli.containsKey("db_schema")) {
         db_schema = cli.get("db_schema");
    } else if (cli.containsKey("output")) {
      output_path = cli.get("output");
    } else if (cli.containsKey("jdbc_output")) {
        jdbc_out_conn = cli.get("jdbc_output");
    } else {
         throw new IllegalArgumentException("Incorrect usage; expected output=... or " +
             "db_schema=... or jdbc_output");
    }

    if (!cli.containsKey("schema")) {
      throw new IllegalArgumentException("Incorrect usage; expected schema=...");
    } else {
      schema_path = cli.get("schema");
    }

    // Get JDBC connection details for output
    if (cli.containsKey("jdbc_output") && !jdbc_out_conn.isEmpty()) {
      if (jdbc_out_conn.substring(0, 7).equals("hdfs://")) {
        try {
          Path pt = new Path(jdbc_out_conn);
          FileSystem fs = FileSystem.get(new Configuration());
          Reader in = new InputStreamReader(fs.open(pt));
          out_prop = new Properties();
          out_prop.load(in);
          jdbc_out_url = out_prop.getProperty("url");
          jdbc_out_schema = out_prop.getProperty("schema");
        } catch (IOException e) {
          throw new IllegalArgumentException("properties file not found in hdfs as specified with" +
              " " + "jdbc_output=...");
        }
      } else {
        try (InputStream in = new FileInputStream(jdbc_out_conn)) {
          out_prop = new Properties();
          out_prop.load(in);
          jdbc_out_url = out_prop.getProperty("url");
          jdbc_out_schema = out_prop.getProperty("schema");
        } catch (IOException e) {
          throw new IllegalArgumentException("properties file not found as specified with " +
              "jdbc_output=...");
        }
      }
    }

    // Sets configuration for the Spark job
    SparkConf conf = new SparkConf()
        .setAppName("DataLake File De-Identification"); // Name of the job
    if (realm_type.equals("local") || realm_type.equals("jdbc-local")) {
      conf.setMaster("local");
    }
    SparkContext sparkContext = new SparkContext(conf);
    SQLContext sqlContext = new SQLContext(sparkContext);
    HiveContext hiveContext = new HiveContext(sparkContext);

    DataFrameTransformFactory transformFactory = new DataFrameTransformFactory(hiveContext);
    SchemaAccessor schemaAccessor = new SchemaAccessor();
    List<FrameConfiguration> configs = Driver.getConfigElements(schemaAccessor, schema_path);

    if ((realm_type.equals("hdfs") || (cli.containsKey("input") && cli.get("input").startsWith
        ("hdfs"))
    )) {
      List<Path> paths = Driver.getCSVFilesInDirectory(new Path(input_path));

      for (Path path : paths) {
        FrameConfiguration configuration = Driver.getMatchingFrameConfig(path, configs);
        TableSchema tableSchema = new TableSchema(configuration.columnStrategies(), transformFactory);

        DataFrame input = hiveContext.read()
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("nullValue", "")
            .option("mode", "PERMISSIVE")
            .option("path", path.toString())
            .load();
        input.show();

        DataFrame output = tableSchema.deidentify(input);

        if (cli.containsKey("output") && output_path != null) {
          if (!CheckPathExists(output_path + "/" + configuration.name())) {
            output.write()
                    .format("com.databricks.spark.csv")
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .option("nullValue", "")
                    .option("mode", "PERMISSIVE")
                    .option("path", output_path + "/" + configuration.name())
                    .save();
          }
        } else if (cli.containsKey("db_schema")) {

          System.out.println("db_schema : " + db_schema);
          System.out.println("table : " + configuration.name());
          output.write()
              .mode(SaveMode.Overwrite)
              .saveAsTable(db_schema + "." + configuration.name());
        } else if (cli.containsKey("jdbc_output")) {
          String table ;
          if (!jdbc_out_schema.isEmpty()) {
            table = jdbc_out_schema + "." + configuration.name();
          } else {
            table = configuration.name();
          }
          output.write()
              .mode(SaveMode.Overwrite)
              .jdbc(jdbc_out_url, table, out_prop);
        }

        output.show();
     }

    } else if (realm_type.equals("local")) {
        List<File> paths = Driver.getCSVFilesInLocalDirectory(input_path);

        for (File path : paths) {
          FrameConfiguration configuration = Driver.getMatchingFrameConfigLocal(path, configs);
          TableSchema tableSchema = new TableSchema(configuration.columnStrategies(), transformFactory);
  
  
          DataFrame input = hiveContext.read()
              .format("com.databricks.spark.csv")
              .option("header", "true")
              .load("file://" + path.toString());
  
          input.show();
  
          DataFrame output = tableSchema.deidentify(input);
  
          if (cli.containsKey("output") && output_path != null) {
            if (!CheckPathExists(output_path + "/" + configuration.name())) {
              output.write()
                      .format("com.databricks.spark.csv")
                      .option("header", "true")
                      .save("file://" + output_path + "/" + configuration.name());
            }
          } else if (cli.containsKey("db_schema")) {

            output.write()
                  .mode(SaveMode.Overwrite)
                  .saveAsTable(db_schema + "." + configuration.name());

          } else if (cli.containsKey("jdbc_output")) {
            String table ;
            if (!jdbc_out_schema.isEmpty()) {
              table = jdbc_out_schema + "." + configuration.name();
            } else {
              table = configuration.name();
            }

            output.write()
                  .mode(SaveMode.Overwrite)
                  .jdbc(jdbc_out_url, table, out_prop);
          }


          output.show();
        }
    } else if (realm_type.equals("jdbc") || realm_type.equals("jdbc-local"))  {

        String[] tables = null;

        if (cli.containsKey("jdbc_input") && !jdbc_in_conn.isEmpty()) {
          if (jdbc_in_conn.substring(0, 7).equals("hdfs://")) {
            try {
              Path pt = new Path(jdbc_in_conn);
              FileSystem fs = FileSystem.get(new Configuration());
              Reader in = new InputStreamReader(fs.open(pt));
              in_prop = new Properties();
              in_prop.load(in);
              jdbc_in_url = in_prop.getProperty("url");
              jdbc_in_schema = in_prop.getProperty("schema");
              jdbc_in_user = in_prop.getProperty("user");
              jdbc_in_password = in_prop.getProperty("password");
              jdbc_in_driver = in_prop.getProperty("driver");
              tables = in_prop.getProperty("table").split(",");
            } catch (IOException e) {
              throw new IllegalArgumentException("properties file not found in hdfs as specified " +
                  "with jdbc_input=...");
            }
          } else {
            try (InputStream in = new FileInputStream(jdbc_in_conn)) {
              in_prop = new Properties();
              in_prop.load(in);
              jdbc_in_url = in_prop.getProperty("url");
              jdbc_in_schema = in_prop.getProperty("schema");
              jdbc_in_user = in_prop.getProperty("user");
              jdbc_in_password = in_prop.getProperty("password");
              jdbc_in_driver = in_prop.getProperty("driver");
              tables = in_prop.getProperty("table").split(",");
            } catch (IOException e) {
              throw new IllegalArgumentException("properties file not found as specified with " +
                  "jdbc_input=...");
            }
          }
        }

        for (String t : tables) {
          File path = new File(t);
          FrameConfiguration configuration = Driver.getMatchingFrameConfigLocal(path, configs);
          TableSchema tableSchema = new TableSchema(configuration.columnStrategies(), transformFactory);


          String jdbc_table ;
          if (!jdbc_in_schema.isEmpty()) {
            jdbc_table = jdbc_in_schema + "." + configuration.name();
          } else {
            jdbc_table = configuration.name();
          }
          Map<String, String> options = new HashMap<String, String>();
          options.put("url", jdbc_in_url);
          options.put("dbtable", jdbc_table);
          options.put("user", jdbc_in_user);
          options.put("password", jdbc_in_password);
          options.put("driver", jdbc_in_driver);

          DataFrame input = hiveContext.read().format("jdbc").options(options).load();

          input.show();

          DataFrame output = tableSchema.deidentify(input);

          if (cli.containsKey("output") && output_path != null) {
            if (!CheckPathExists(output_path + "/" + configuration.name())) {
              output.write()
                      .format("com.databricks.spark.csv")
                      .option("header", "true")
                      .option("inferSchema", "true")
                      .option("nullValue", "")
                      .option("mode", "PERMISSIVE")
                      .option("path", output_path + "/" + configuration.name())
                      .save();
            }
          } else if (cli.containsKey("db_schema")) {

            System.out.println("db_schema : " + db_schema);
            System.out.println("table : " + configuration.name());
            output.write()
                .mode(SaveMode.Overwrite)
                .saveAsTable(db_schema + "." + configuration.name());
          } else if (cli.containsKey("jdbc_output")) {
            String table ;
            if (!jdbc_out_schema.isEmpty()) {
              table = jdbc_out_schema + "." + configuration.name();
            } else {
              table = configuration.name();
            }
            output.write()
                .mode(SaveMode.Overwrite)
                .jdbc(jdbc_out_url, table, out_prop);
          }

          output.show();
        }
    }

    sparkContext.stop();
  }

  private static List<Path> getCSVFilesInDirectory(Path directory) throws IOException {

    try {
      List<Path> paths = new ArrayList<>();
      FileSystem fs = FileSystem.get(new Configuration());
      //TODO: apply filters...ends with csv
      RemoteIterator<LocatedFileStatus> filestatus = fs.listFiles(directory, false);
      while (filestatus.hasNext()) {
        paths.add(filestatus.next().getPath());
      }
      return paths;
    } catch (IOException ex) {
      logger.error(ex);
      logger.error("Failed to list all files in given input directory to Spark context. Failing.");
      throw ex;
    }
  }

  private static List<File> getCSVFilesInLocalDirectory(String directory) throws
      IOException {

    try {
      List<File> paths = new ArrayList<>();
      File[] files = new File(directory).listFiles();
      for (File file : files) {
        paths.add(file);
      }
      if (paths.size() == 0){
        throw new java.io.IOException("No files in directory ...");
      }
      return paths;
    } catch (IOException ex) {
      logger.error(ex);
      logger.error("Failed to list all files in given input directory to Spark context. Failing.");
      throw ex;
    }
  }

  private static FrameConfiguration getMatchingFrameConfig(Path file, List<FrameConfiguration> choices) {
    for (FrameConfiguration config : choices) {
      if (file.getName().toString().equalsIgnoreCase(config.name())) {
        return config;
      }
    }

    for (FrameConfiguration config : choices) {
      if (file.getName().toString().matches(String.format("(?i:.*%s.*)", config.name()))) {
        return config;
      }
    }

    String error = String.format("Could not find a configuration element for file %s",
        file.toString());

    logger.error(error);
    throw new IllegalStateException(error);
  }

  private static FrameConfiguration getMatchingFrameConfigLocal(File file,
                                                                List<FrameConfiguration> choices) {
    for (FrameConfiguration config : choices) {
      if (file.getName().toString().equalsIgnoreCase(config.name())) {
        return config;
      }
    }

    for (FrameConfiguration config : choices) {
      if (file.getName().toString().matches(String.format("(?i:.*%s.*)", config.name()))) {
        return config;
      }
    }

    String error = String.format("Could not find a configuration element for file %s",
        file.toString());

    logger.error(error);
    throw new IllegalStateException(error);
  }

  static List<FrameConfiguration> getConfigElements(SchemaAccessor schemaAccessor, String
      schema_path) {

    if (schema_path.substring(0, 7).equals("hdfs://")) {
      try {
        Path pt = new Path(schema_path);
        FileSystem fs = FileSystem.get(new Configuration());
        Reader schemaConfig = new InputStreamReader(fs.open(pt));
        return schemaAccessor.readMultipleConfiguration(schemaConfig);
      } catch (FileNotFoundException iex) {
        logger.error("schema configuration file path not found", iex);
        throw new IllegalStateException("Failed to open schema configuration file", iex);
      } catch (IOException iex) {
        logger.error("Could not open schema configuration file", iex);
        throw new IllegalStateException("Failed to open schema configuration file", iex);
      }
    }

    try (InputStream schema = new FileInputStream(schema_path);
         Reader schemaConfig = new InputStreamReader(schema)) {
      return schemaAccessor.readMultipleConfiguration(schemaConfig);
    } catch (FileNotFoundException iex) {
      logger.error("schema configuration file path not found", iex);
      throw new IllegalStateException("Failed to open schema configuration file", iex);
    } catch (IOException iex) {
      logger.error("Could not open schema configuration file", iex);
      throw new IllegalStateException("Failed to open schema configuration file", iex);
    }

  }

  public static boolean CheckPathExists(String hdfs_path) {
    Runtime r = Runtime.getRuntime();
    int procExitValue = 0;

    try {
      Process p0 = r.exec("hdfs dfs -test -e " + hdfs_path );
      p0.waitFor();
      procExitValue = p0.exitValue();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return (procExitValue == 0 ? true:false);
  }

}

