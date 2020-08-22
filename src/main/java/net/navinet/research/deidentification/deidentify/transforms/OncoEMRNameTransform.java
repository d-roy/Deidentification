package net.navinet.research.deidentification.deidentify.transforms;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import java.io.IOException;
import java.io.InputStream;
import java.security.SecureRandom;
import java.util.List;

import static org.apache.spark.sql.functions.callUDF;

final class OncoEMRNameTransform extends AbstractDataFrameTransform {
  static private List<String> temp_male_name_array = male_name_reader();
  static private List<String> temp_female_name_array = female_name_reader();
  static final int male_num_names = temp_male_name_array.size();
  static final int female_num_names = temp_female_name_array.size();
  static final SecureRandom rnd = new SecureRandom();

  private static final Logger logger = Logger.getLogger(OncoEMRNameTransform.class.getName());

  OncoEMRNameTransform(String columnName) {
    super(columnName);
  }


  /**
   *  parse through the text file
   * @return lines that needed to be read
   */
  static private List<String> male_name_reader(){
    try (InputStream input =
             (FirstNameTransform.class.getClassLoader().getResourceAsStream
                 ("net/navinet/research/spark/oncoemr/male_names"))) {
      return IOUtils.readLines(input,"UTF-8");
    }
    catch (IOException e){
      e.printStackTrace();
      logger.error(e);
      return null;
    }
  }

  /**
   *  parse through the text file
   * @return lines that needed to be read
   */
  static private List<String> female_name_reader() {
    try (InputStream input =
             (FirstNameTransform.class.getClassLoader().getResourceAsStream
                 ("net/navinet/research/spark/oncoemr/female_names"))) {
      return IOUtils.readLines(input,"UTF-8");
    }
    catch (IOException e){
      e.printStackTrace();
      return null;
    }
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
    sqlContext.udf().register("oncoemrFirstName", new UDF2<Object, Object, String>() {
      @Override
      public String call(Object name, Object gender) throws Exception {
        if (name == null) {
          return null;
        }

        String new_name = null;

        if (gender != null && gender.toString().toLowerCase().equals("female")) {
          new_name = temp_female_name_array.get(rnd.nextInt(female_num_names));
          if (new_name.equalsIgnoreCase(name.toString()))
            new_name = temp_female_name_array.get(rnd.nextInt(female_num_names));
          return new_name;
        }

        new_name = temp_male_name_array.get(rnd.nextInt(male_num_names));
        if (new_name.equalsIgnoreCase(name.toString()))
          new_name = temp_male_name_array.get(rnd.nextInt(male_num_names));
        return new_name;
      }
    }, DataTypes.StringType);
  }

  @Override
  public DataFrame transform(DataFrame input) {
    return input.withColumn(columnName, callUDF("oncoemrFirstName", input.col(columnName), input
        .col("gender")));
  }
}
