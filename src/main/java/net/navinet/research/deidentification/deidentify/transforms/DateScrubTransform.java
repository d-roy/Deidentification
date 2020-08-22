package net.navinet.research.deidentification.deidentify.transforms;

import static org.apache.spark.sql.functions.callUDF;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * A class for removing all date-related information except for years, unless the year indicates
 * that a person is at least 90 years old
 */
final class DateScrubTransform extends AbstractDataFrameTransform {

  /**
   * Basic constructor for the DateEpochTransform class
   *
   * @param columnName the name of the column to be transformed
   */
  DateScrubTransform(String columnName) {
    super(columnName);
  }

  /**
   * A peculiarity of the Java API for Spark is that user-defined functions must
   * be registered against the Spark context against which it is to be run. The
   * reason, for this, is that it is necessary to work with Java's type system.
   * <p>
   * In any case, applying a user-defined function to a column is a two-step
   * procedure, as per
   * <code>
   * sqlContext.udf().register("fn", fn, DataType);
   * dataFrame.withColumn(columnName, callUDF("fn", dataFrame.col(columnName));
   * </code>
   * <p>
   * This mapping produces a new Date object with the same year, but month and day are set to 1.
   *
   * @param sqlContext {@link SQLContext} to register this mapping to
   */
  static void register(SQLContext sqlContext) {
    sqlContext.udf().register("DateScrub", new UDF1<Object, Timestamp>() {
      @Override
      public Timestamp call(Object col) throws Exception {
        if (col == null) {
          return null;
        }

        SimpleDateFormat dashFormat = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat slashFormat = new SimpleDateFormat("M/d/yyyy");
        String input = col.toString();

        Calendar inputDate = new GregorianCalendar();
        if (input.matches("\\d{4}-\\d{2}-\\d{2}")) {
          inputDate.setTime(dashFormat.parse(input));
        } else if (input.matches("\\d{1,2}/\\d{1,2}/\\d{4}")) {
          inputDate.setTime(slashFormat.parse(input));
        }

        int year = inputDate.get(Calendar.YEAR);
        Calendar cal = Calendar.getInstance();
        int first = cal.get(Calendar.YEAR) - 90;
        long instant;
        if (year > first) {
          instant = new GregorianCalendar(year, 0, 1).getTimeInMillis();
        } else {
          instant = new GregorianCalendar(first, 0, 1).getTimeInMillis();
        }

        return new Timestamp(instant);
      }
    }, DataTypes.TimestampType);
  }

  @Override
  public DataFrame transform(DataFrame input) {
    return input
        .withColumn(
            columnName,
            callUDF("DateScrub", input.col(columnName)));
  }
}