package net.navinet.research.deidentification.deidentify.transforms;

import net.navinet.research.spark.dlake.Driver;

import org.apache.log4j.LogManager;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import org.apache.log4j.Logger;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.security.SecureRandom;

import static org.apache.spark.sql.functions.callUDF;

/**
 * Transform routine to modify the day part of a date field
 */

final class OncoEMRDateShiftTransform extends AbstractDataFrameTransform {
  static final Logger logger = LogManager.getLogger(OncoEMRDateShiftTransform.class.getName());
  static final int[] num_days_shift = {-8,-7,-6,-5,-4,8,7,6,5,4};
  static final SecureRandom rnd = new SecureRandom();
  static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

  OncoEMRDateShiftTransform(String columnName) {
    super(columnName);
  }

  static void register(SQLContext sqlContext) {
    sqlContext.udf().register("DateShiftTransform", new UDF2<Object, Object, String>() {
      @Override
      public String call(Object dt, Object patientid) throws Exception {
        int days_to_shift;

        if (dt == null) {
           return DateDeIdentify(null, 0);
        }

        if (patientid == null) {
          days_to_shift = num_days_shift[rnd.nextInt(10)];
          return DateDeIdentify(dt.toString(), days_to_shift);
        }
        String patient_id = patientid.toString();
        char patient_group = patient_id.charAt(patient_id.length() - 1);
        int ind = Character.getNumericValue(patient_group);
        days_to_shift = num_days_shift[ind];
        String p_dt = dt.toString();
        return DateDeIdentify(p_dt, days_to_shift);
      }
    }, DataTypes.StringType);
  }

  private static String DateDeIdentify(String dt, int num_days) throws ParseException  {

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
    cal.add(Calendar.DATE, num_days);

    return dateFormat.format(cal.getTime());
  }

  @Override
  public DataFrame transform(DataFrame input) {
    return input.withColumn(columnName, callUDF("DateShiftTransform", input.col(columnName),
        input.col("patientid"))
        .cast("date"));
  }
}
