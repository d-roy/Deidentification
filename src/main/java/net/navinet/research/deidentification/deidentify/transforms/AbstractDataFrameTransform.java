package net.navinet.research.deidentification.deidentify.transforms;

/**
 * Created by KAllen on 6/10/2016.
 */
abstract class AbstractDataFrameTransform implements DataFrameTransform {

  // name of the column to be transformed
  protected final String columnName;

  AbstractDataFrameTransform(String columnName) {
    this.columnName = columnName;
  }
}
