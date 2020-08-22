package net.navinet.research.deidentification.deidentify.transforms;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import static org.apache.spark.sql.functions.callUDF;

/**
 * Transform routine to modify the day part of a date field
 */

final class DateDayTransform extends AbstractDataFrameTransform {
  static final Logger logger = LogManager.getLogger(DateDayTransform.class.getName());
  static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

  DateDayTransform(String columnName) {
    super(columnName);
  }

  static void register(SQLContext sqlContext) {
    sqlContext.udf().register("DateDayTransform", new UDF1<Object, String>() {
      @Override
      public String call(Object s) throws Exception {
        if (s == null) {
          return DateDeIdentify(null);
        } else if(s.toString().equalsIgnoreCase("null")) {
          return DateDeIdentify(null);
        }
        else {
          return DateDeIdentify(s.toString());
        }
      }
    }, DataTypes.StringType);
  }

  private static String DateDeIdentify(String dt) throws ParseException {

    if (dt == null) {
      return null;
    }

    Date date = null;
    try {
      date = dateFormat.parse(dt);
    } catch (ParseException e) {
      System.out.println("Date parsing exception ...");
      logger.error(e);
      throw e;
    }
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);

    int column_year = cal.get(Calendar.YEAR);

    Calendar curr_cal = Calendar.getInstance();
    curr_cal.setTime(new Date());
    int curr_year = curr_cal.get(Calendar.YEAR);

    if ((curr_year - column_year) > 89)
    {
      int year_to_be = curr_year - 89;
      cal.set(Calendar.YEAR, year_to_be);
    }

    cal.set(Calendar.MONTH, 0);
    cal.set(Calendar.DAY_OF_MONTH, 1);

    return dateFormat.format(cal.getTime());
  }

  @Override
  public DataFrame transform(DataFrame input) {
    return input.withColumn(columnName, callUDF("DateDayTransform", input.col(columnName)).cast("timestamp"));
  }
}
