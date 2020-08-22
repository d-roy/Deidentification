package net.navinet.research.deidentification.deidentify.transforms;

import org.apache.spark.sql.DataFrame;
import static org.apache.spark.sql.functions.lit;

/**
 * Transform to replace the indicated column with the string "FirstName"
 */
final class FixedFirstNameTransform extends AbstractDataFrameTransform {

  public FixedFirstNameTransform(String columnName) {
    super(columnName);
  }

  @Override
  public DataFrame transform(DataFrame input) {
    return input.withColumn(columnName, lit("FirstName"));
  }
}
