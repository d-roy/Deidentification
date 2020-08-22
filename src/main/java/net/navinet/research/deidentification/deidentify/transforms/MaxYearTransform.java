package net.navinet.research.deidentification.deidentify.transforms;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.DataFrame;

import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * Remove references to years more than 90 years ago, according to HIPAA regulations. In particular,
 * for all years less than ninty years ago, it will return the given year; for all years greater
 * than or equal to ninty years ago, it will return the year ninty years ago (from when the program
 * was run).
 */
final class MaxYearTransform extends AbstractDataFrameTransform {

  public MaxYearTransform(String columnName) {
    super(columnName);
  }

  @Override
  public DataFrame transform(DataFrame input) {
    return input.withColumn(columnName,
      when(input.col(columnName).isNull(), lit(null))
        .otherwise(greatest(
          input.col(columnName),
          lit(new GregorianCalendar().get(Calendar.YEAR) - 90)
          )
        ));
  }
}
