package net.navinet.research.deidentification.schema;

import net.navinet.research.deidentification.connection.Connection;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * Connection designed to be used in tests of TableSchema.
 */
public class ListConnection implements Connection {
  private final SQLContext sqlContext;
  private final String path;

  private String name;
  private Row[] rows;

  public ListConnection(SQLContext sqlContext, String path) {
    this.sqlContext = sqlContext;
    this.path = path;
  }


  @Override
  public DataFrameReader readFrom() {
    return sqlContext
        .read()
        .format("json")
        .option("path", this.path);
  }

  @Override
  public void saveTo(String name, DataFrame contents) {
    this.name = name;
    this.rows = contents.collect();
  }

  public String getSavedName() {
    return this.name;
  }

  public Row[] getSavedContents() {
    return this.rows;
  }
}
