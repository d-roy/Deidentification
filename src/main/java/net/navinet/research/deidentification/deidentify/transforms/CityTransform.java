package net.navinet.research.deidentification.deidentify.transforms;

import static org.apache.spark.sql.functions.callUDF;

import org.apache.commons.io.*;
import org.apache.log4j.Logger;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;


/**
 * Transforms current city into another random city with the same zip code
 */

final class CityTransform extends AbstractDataFrameTransform {

   static private Map<String, List<String>> data = new HashMap<String, List<String>>();
   static String splitBy = ",";
   private static final Logger logger = Logger.getLogger(CityTransform.class.getName());


  CityTransform(String columnName) {
    super(columnName);
  }

    /**
     *  parse through the csv file
     * @return lines that needed to be read
     */
    static private List<String> linereader(){

        try {

            InputStream input =
                    (CityTransform.class.getClassLoader().getResourceAsStream("us_postal_codes_full"));
            return IOUtils.readLines(input,"UTF-8");
        }
        catch (IOException e){
            e.printStackTrace();
            return null;
        }
    }

    /**
     * method that consumes all the data from the csv files and creates a map that
     * has zip codes as keys and a list of cities as values.
     */

    static private void setData(){
        try {
            List<String> lines = linereader();
            for (String line : lines) {
                String[] array = line.split(splitBy);
                String zipcode = array[0];
                String subString = zipcode.substring(0, 3);
                String city = array[1];
                List<String> valuesList = data.get(subString);
                if (valuesList == null) {
                    valuesList = new ArrayList<>();
                    valuesList.add(city);
                    data.put(subString, valuesList);
                }
                else{
                    valuesList.add(city);
                }
            }
        }
        catch (NullPointerException iex) {
                logger.error("Failed to open postal code file. Stopping job.");
                throw new IllegalStateException("Failed to open postal code file. Stopping job.");
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
     * This mapping randomly picks a zip code from the list of similar random zip codes
     *
     * @param sqlContext {@link SQLContext} to register this mapping to
     */
      static void register(SQLContext sqlContext) {
          setData();
          sqlContext.udf().register("CityTransform", new UDF1<Object, String>() {
              @Override
              public String call(Object i) throws Exception {
            	  if(i == null) {
            		  return null;
            	  }
            	  
                  List<String> cities;
                  try {
                      cities = data.get(i.toString().substring(0, 3));
                  } catch (Exception ex) {
                      return "that is not a valid zipcode";
                  }
                  if(cities == null) {
                	  return "unknown";
                  }
                  final int index = ThreadLocalRandom.current().nextInt(cities.size());
                  for(String city: cities) {
                      if (city == null) {
                          return "unknown";
                      }
                  }
                  return cities.get(index);
              }

          }, DataTypes.StringType);
      }

  /**
   * Apply the mapping represented by this object to the given {@link DataFrame}.
   *
   * @param input {@code DataFrame} to be transformed by this mapping
   * @return {@code DataFrame} after applying the mapping
   */
  @Override
  public DataFrame transform(DataFrame input) {
    return input.withColumn(columnName, callUDF("CityTransform", input.col(columnName)));
  }
}