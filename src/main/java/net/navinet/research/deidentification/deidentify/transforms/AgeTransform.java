package net.navinet.research.deidentification.deidentify.transforms;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.DataFrame;

/**
 * Returns the given age, capping out at 90.
 */
final class AgeTransform extends AbstractDataFrameTransform {

  AgeTransform(String columnName) {
    super(columnName);
  }

  @Override
  public DataFrame transform(DataFrame input) {
    return input.withColumn(columnName,
        when(input.col(columnName).isNull(), lit(null))
        .when(input.col(columnName).isNaN(), lit(null))
            .otherwise(least(input.col(columnName), lit(90))));
  }
}
