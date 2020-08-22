package net.navinet.research.deidentification.connection;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import java.util.Properties;

/**
 * Represents a connection to a database (or data source in general) which can be accessed via
 * JDBC.
 */
// TODO This class should not be public. That it is public indicates a serious design problem.
public final class JDBCConnection implements Connection {
  private static final Logger logger = LogManager.getLogger(JDBCConnection.class);

  private final SQLContext sqlContext;
  private final String url;
  private final String userName;
  private final String password;

  JDBCConnection(SQLContext sqlContext, String url, String userName, String password) {
    this.sqlContext = sqlContext;
    this.url = url;
    this.userName = userName == null ? "" : userName;
    this.password = password == null ? "" : password;
  }

  @Override
  public DataFrameReader readFrom() {
    return this.sqlContext.read()
        .format("jdbc")
        .option("url", this.url)
        .option("user", this.userName)
        .option("password", this.password);
  }

  public DataFrame read(String tableName, String partitionColumn) {
    Properties properties = new Properties();
    properties.setProperty("user", this.userName);
    properties.setProperty("password", this.password);

    if (partitionColumn == null) {
      return this.sqlContext.read().jdbc(this.url, tableName, properties);
    } else {
      Row sizeInformation = this.collectSizeInformation(tableName, partitionColumn);

      int partitions = this.sqlContext.sparkContext().defaultParallelism();

      Long maxId = sizeInformation.getAs("max_id");
      Long minId = sizeInformation.getAs("min_id");

      logger.info(
        String.format(
          "Making partitioned request to table with max_id %d and min_id %d",
          maxId, minId));
      return this.sqlContext.read()
        .jdbc(this.url, tableName, partitionColumn, minId, maxId, partitions, properties);
    }
  }

  @Override
  public void saveTo(String name, DataFrame contents) {
    System.out.println(name);
    contents.show();
  }

  @Override
  public String toString() {
    return String.format("[JDBC - %s]", this.url);
  }

  /**
   *
   * @param table Name of the table to be accessed
   * @param partitionColumn Name of column to be used as basis for partitioning
   * @return A single row with elements min_id and max_id
   */
  private Row collectSizeInformation(String table, String partitionColumn) {
    DataFrame stats = sqlContext.read().format("jdbc")
      .option("url", this.url)
      .option("dbtable",
        String.format("(SELECT max(%s) AS max_id, min(%<s) AS min_id FROM %s) temp_table",
          partitionColumn, table))
      .option("user", this.userName)
      .option("password", this.password)
      .load();

    return stats.collect()[0];
  }
}
