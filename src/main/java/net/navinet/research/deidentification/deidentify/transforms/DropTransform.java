package net.navinet.research.deidentification.deidentify.transforms;

import org.apache.spark.sql.DataFrame;

/**
 * Removes the column with the given columnName from the DataFrame, irrespective of type.
 */
final class DropTransform extends AbstractDataFrameTransform {

  /**
   * Basic constructor for DropTransform class.
   *
   * @param columnName Name of the column to be dropped.
   */
  public DropTransform(String columnName) {
    super(columnName);
  }

  @Override
  public DataFrame transform(DataFrame input) {
    return input.drop(columnName);
  }
}
