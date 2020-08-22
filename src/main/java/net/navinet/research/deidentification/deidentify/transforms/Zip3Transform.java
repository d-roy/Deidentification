package net.navinet.research.deidentification.deidentify.transforms;

import static org.apache.spark.sql.functions.callUDF;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

/**
 * Transforms Zip Codes to be in compliance of HIPAA regulations.
 */
final class Zip3Transform extends AbstractDataFrameTransform {

  /**
   * Basic constructor for the Zip3Transform.
   *
   * @param columnName name of the column to be transformed.
   */
  Zip3Transform(String columnName) {
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
   * This mapping reproduces a zipcode without the final 2 digits, and if the first 3
   * digits are not allowed, scrubs those as well.
   *
   * @param sqlContext {@link SQLContext} to register this mapping to
   */
  static void register(SQLContext sqlContext) {
    sqlContext.udf().register("Zip3", new UDF1<Object, String>() {
      @Override
      public String call(Object s) throws Exception {
        if (s == null) {
          return allowedZip(null);
        } else if(s.toString().equalsIgnoreCase("null")) {
        	return allowedZip(null);
        }
        else {
          return allowedZip(s.toString());
        }
      }
    }, DataTypes.StringType);
  }

  /**
   * Specific to de-identification of zip codes. The initial 3 digits of a zip code may be
   * retained, unless they are in a specific list of 17 codes which contain less than 20,000
   * individuals. As a result, these 17 zip codes must be filtered and removed. In these cases, the
   * placeholder "000" is returned.
   *
   * @param zip the provided zip code
   * @return the original zip code, or "000" if the zip is not allowed
   */
  private static String allowedZip(String zip) {

    if (zip == null) {
      return null;
    }

    String zp;
    if (zip.length() <= 3) {
      zp = StringUtils.rightPad(zip, 3, "0");
    } else {
      zp = zip.substring(0, 3);
    }

    // the 17 restricted zip codes
    String[] arr = {"036", "692", "878", "059", "790", "879", "063", "821", "884",
        "102", "823", "890", "203", "830", "893", "556", "831"};

    // checks if the zip is contained in the array of restricted zip codes
    for (String anArr : arr) {
      // return the base zip
      if (anArr.equals(zp)) {
        return "00000";
      }
    }

    // return the original zip code
    return StringUtils.rightPad(zp, 5, "0");
  }

  @Override
  public DataFrame transform(DataFrame input) {
    return input.withColumn(columnName, callUDF("Zip3", input.col(columnName)));
  }
}
