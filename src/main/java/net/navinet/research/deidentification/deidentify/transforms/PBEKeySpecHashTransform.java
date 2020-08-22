package net.navinet.research.deidentification.deidentify.transforms;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

/**
 * Perform a hash on the column using the PBEKeySpec algorithm.
 */
final class PBEKeySpecHashTransform extends AbstractDataFrameTransform {
  /**
   * The salt for this hash.
   */
  private final String salt;

  /**
   * Basic constructor for HashTransform class.
   *
   * @param columnName Name of the column which will be transformed.
   */
  public PBEKeySpecHashTransform(String columnName, String salt) {
    super(columnName);
    this.salt = salt;
  }

  /**
   * A peculiarity of the Java API for Spark is that user-defined functions must
   * be registered against the Spark context against which it is to be run. The
   * reason, for this, is that it is necessary to work with Java's type system.
   * <p>
   * In any case, applying a user-defined function to a column is a two-step
   * procedure, as per:
   * <code>
   * sqlContext.udf().register("fn", fn, DataType);
   * dataFrame.withColumn(columnName, callUDF("fn", dataFrame.col(columnName));
   * </code>
   * <p>
   * This register returns a new String that has been put through a secure hashing function.
   *
   * @param sqlContext {@link SQLContext} to register this mapping
   */
  public static void register(SQLContext sqlContext) {
    sqlContext.udf().register("PBEKeyHash", new UDF2<Object, String, String>() {
      @Override
      public String call(Object column, String salt) throws Exception {
        if (column == null) {
          return null;
        } else if (column.toString().isEmpty()) {
          return "";
        } else {
          byte[] salts = salt.getBytes();
          char[] chars = column.toString().trim().toCharArray();
          PBEKeySpec spec = new PBEKeySpec(chars, salts, 2016, 64 * 8);
          SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
          SecretKey key = keyFactory.generateSecret(spec);
          return DataFrameTransformFactory.toHex(key.getEncoded());
        }
      }
    }, DataTypes.StringType);
  }

  @Override
  public DataFrame transform(DataFrame input) {
    Column target = input.col(columnName);

    return input.withColumn(columnName, callUDF("PBEKeyHash", target, lit(this.salt)));
  }
}

