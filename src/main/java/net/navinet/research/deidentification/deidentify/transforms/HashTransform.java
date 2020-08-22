package net.navinet.research.deidentification.deidentify.transforms;

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
 * Hash transformation. Maps a String to a String put through a Hash function. Purpose is obfuscating PHI
 * or other information.
 */
final class HashTransform extends AbstractDataFrameTransform {

  /**
   * The salt for this hash.
   */
  private final String salt;

  /**
   * Basic constructor for HashTransform class.
   *
   * @param columnName Name of the column which will be transformed.
   */
  HashTransform(String columnName, String salt) {
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
  }

  @Override
  public DataFrame transform(DataFrame input) {
    Column target = input.col(columnName);

    return input.withColumn(columnName,
      when(target.isNull(), lit(null))
        .otherwise(when(target.equalTo(""), lit(""))
          .otherwise(sha2(concat(trim(target), lit(salt)), 512))));
  }
}

