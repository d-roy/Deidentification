package net.navinet.research.deidentification.deidentify.transforms;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;
import org.apache.spark.sql.api.java.UDF1;
import static org.apache.spark.sql.functions.callUDF;

import java.util.Random;

/**
 * Identity Transform. Maps a String onto itself. Principally used
 * as a demonstration of of how to use column-to-column mappings.
 */
final class IdentityTransform extends AbstractDataFrameTransform {

  /**
   * Simple constructor for the IdentityTransform object
   *
   * @param columnName the name of the column to be transformed.
   */
  public IdentityTransform(String columnName) {
    super(columnName);
  }

  static void register(SQLContext sqlContext) {
    sqlContext.udf().register("CleanseData", new UDF1<String, String>() {
      @Override
      public String call(String content) throws Exception {
        if (content == null) {
          return null;
        }
        String transformedContent = content.replace("\n", " ").replace("\r", " ");
        return transformedContent;
      }
    }, DataTypes.StringType);
  }

  @Override
  public DataFrame transform(DataFrame input) {
    for (Tuple2<String, String> tup : input.dtypes()) {
//    System.out.println("columns and types : " + tup.toString());
      if (tup._1.equalsIgnoreCase(columnName) && tup._2.equalsIgnoreCase("StringType")) {
        return input.withColumn(columnName, callUDF("CleanseData", input.col(columnName)));
      }
    }
    return input;
  }
}
