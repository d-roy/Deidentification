package net.navinet.research.deidentification.connection;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameReader;

/**
 * Created by pteixeira on 6/14/2016.
 */
public interface Connection {
  DataFrameReader readFrom();

  void saveTo(String name, DataFrame contents);
}
