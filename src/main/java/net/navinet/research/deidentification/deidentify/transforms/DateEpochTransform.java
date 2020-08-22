package net.navinet.research.deidentification.deidentify.transforms;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.DataFrame;

/**
 * A class for removing all date-related information except for years, unless the year indicates
 * that a person is at least 90 years old.
 */
final class DateEpochTransform extends AbstractDataFrameTransform {
  private final String epochStart;

  /**
   * Basic constructor for the DateEpochTransform class.
   *
   * @param columnName the name of the column to be transformed
   */
  DateEpochTransform(String columnName, String epochStart) {
    super(columnName);
    this.epochStart = epochStart;
  }

  @Override
  public DataFrame transform(DataFrame input) {
    return input.withColumn(
        columnName,
        when(input.col(columnName).isNull(), lit(null))
            .when(input.col(epochStart).isNull(), lit(null))
            .otherwise(datediff(input.col(columnName), input.col(epochStart))));
  }
}