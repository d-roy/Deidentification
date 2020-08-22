package net.navinet.research.deidentification.deidentify.transforms;

import org.apache.spark.sql.DataFrame;
import static org.apache.spark.sql.functions.lit;

/**
 * Transform to replace the indicated column with the string "LastName"
 */
final class FixedLastNameTransform extends AbstractDataFrameTransform {

  FixedLastNameTransform(String columnName) {
    super(columnName);
  }

  @Override
  public DataFrame transform(DataFrame input) {
    return input.withColumn(columnName, lit("LastName"));
  }
}
