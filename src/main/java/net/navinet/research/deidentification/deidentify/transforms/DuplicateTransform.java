package net.navinet.research.deidentification.deidentify.transforms;

import org.apache.spark.sql.DataFrame;

/**
 * Duplicates the column with the given columnName irrespective of type. Principle purpose is
 * to demonstrate enrichment of the table.
 */
final class DuplicateTransform extends AbstractDataFrameTransform {

  /**
   * Basic constructor for DuplicateTransform class.
   *
   * @param columnName name of the column to be duplicated.
   */
  public DuplicateTransform(String columnName) {
    super(columnName);
  }

  @Override
  public DataFrame transform(DataFrame input) {
    return input.withColumn(columnName + "_duplicate", input.col(columnName));
  }
}
