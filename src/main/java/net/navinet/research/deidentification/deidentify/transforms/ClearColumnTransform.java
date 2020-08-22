package net.navinet.research.deidentification.deidentify.transforms;

import org.apache.spark.sql.DataFrame;
import static org.apache.spark.sql.functions.lit;
/**
 * Transform to clear the content of the indicated column
 */

final class ClearColumnTransform extends AbstractDataFrameTransform {

  ClearColumnTransform(String columnName) {
    super(columnName);
  }

  @Override
  public DataFrame transform(DataFrame input) {
    return input.withColumn(columnName, lit(" "));
  }
}
