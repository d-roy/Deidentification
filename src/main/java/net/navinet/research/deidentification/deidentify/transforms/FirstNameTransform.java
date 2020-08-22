package net.navinet.research.deidentification.deidentify.transforms;

import static org.apache.spark.sql.functions.callUDF;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.util.Random;

/**
 * Created by KAllen on 6/13/2016.
 */
final class FirstNameTransform extends AbstractDataFrameTransform {

  static final String[] firstNames =
      {"James",
          "John",
          "Robert",
          "Michael",
          "William",
          "David",
          "Richard",
          "Charles",
          "Joseph",
          "Thomas",
          "Christopher",
          "Daniel",
          "Paul",
          "Mark",
          "Donald",
          "George",
          "Kenneth",
          "Steven",
          "Edward",
          "Brian",
          "Ronald",
          "Anthony"
      };

  FirstNameTransform(String columnName) {
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
   * This mapping creates a random firstname for every element in the column.
   *
   * @param sqlContext {@link SQLContext} to register this mapping to
   */
  static void register(SQLContext sqlContext) {
    final Random r = new Random();
    sqlContext.udf().register("FirstName", new UDF1<String, String>() {
      @Override
      public String call(String string) throws Exception {
        return firstNames[r.nextInt(firstNames.length)];
      }
    }, DataTypes.StringType);
  }

  @Override
  public DataFrame transform(DataFrame input) {
    return input.withColumn(columnName, callUDF("FirstName", input.col(columnName)));
  }
}
