package net.navinet.research.deidentification.deidentify.transforms;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.to_date;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.sql.Date;
import java.util.GregorianCalendar;
import java.util.Random;


/**
 * A class used to create a new column for a randomized date of within 2 years of the original date. No change
 * is made to the original column.
 */
public class EpochStartTransform extends AbstractDataFrameTransform {
  public EpochStartTransform(String columnName) {
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
   * This mapping randomizes the date of the given column within 2 years.
   *
   * @param sqlContext {@link SQLContext} to register this mapping to
   */
  static void register(SQLContext sqlContext) {
    sqlContext.udf().register("EpochStart", new UDF1<Object, Date>() {
          @Override
          public Date call(Object col) throws Exception {
            return randomizeDate(col);
          }
        },
        DataTypes.DateType);
  }

  /**
   * Randomizes the date within 2 years.
   *
   * @param col the date object to be changed
   * @return the altered date object
   */
  private static Date randomizeDate(Object col) {
    // checks for null
    if (col == null) return null;

    // turns the date into a string
    String date = col.toString();

    // places the year, month, and day values in an array
    String[] arr = date.split(" ")[0].split("T")[0].split("-");

    // retrieves the important values from the date
    int year = Integer.parseInt(arr[0]);
    int month = Integer.parseInt(arr[1]);
    int day = Integer.parseInt(arr[2]);

    // retrieves the number of milliseconds away from Jan. 1st 1970 for the given date
    long ms = new GregorianCalendar(year, month, day).getTimeInMillis();

    // retrieves the difference in milliseconds to be added to the original date
    Random r = new Random();
    long diff = (long) ((31536000000L * 4) * r.nextDouble()) - (31536000000L * 2);

    // creates the new date
    long instant = ms + diff;
    return new Date(instant);
  }

  @Override
  public DataFrame transform(DataFrame input) {
    return input.withColumn(
        "EPOCH_START",
        to_date(callUDF("EpochStart", input.col(columnName))));
  }
}
