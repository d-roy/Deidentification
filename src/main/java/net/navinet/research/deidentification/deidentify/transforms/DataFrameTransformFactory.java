package net.navinet.research.deidentification.deidentify.transforms;

import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import net.navinet.research.deidentification.deidentify.ColumnStrategy;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SQLContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

/**
 * Factory for creating an {@link DataFrameTransform}.
 */
public final class DataFrameTransformFactory {
  private final Logger logger = Logger.getLogger(this.getClass());

  private final Map<String, String> map = new HashMap<>();

  public DataFrameTransformFactory(SQLContext sqlContext) {
    Objects.requireNonNull(sqlContext);

    this.registerTransforms(sqlContext);

    try (FileReader saltFileReader = new FileReader("saltConfigs.json")) {
      Gson gson = new Gson();
      Type stringStringMap = new TypeToken<Map<String, String>>() {
      }.getType();
      Map<String, String> map = gson.fromJson(saltFileReader, stringStringMap);
      if (map != null) {
        this.map.putAll(map);
      }
    } catch (FileNotFoundException e) {
      logger.warn("Failed to open ths salt configuration file; starting with empty salt file.");
    } catch (IOException e) {
      logger.error("An IO error occured while reading the salt file. Stopping.", e);
      throw new IllegalStateException("Failed to read salt file.");
    }
  }

  /**
   * Converts a byte array into a String of hexadecimal characters.
   *
   * @param array The array of bytes to be converted.
   * @return The hexadecimal String.
   */
  static String toHex(byte[] array) {
    BigInteger bi = new BigInteger(1, array);
    String hex = bi.toString(16);
    int paddingLength = (array.length * 2) - hex.length();
    if (paddingLength > 0) {
      return String.format("%0" + paddingLength + "d", 0) + hex;
    } else {
      return hex;
    }
  }

  /**
   * Return the transformation which can be applied to a {@code DataFrame} which will produce the
   * de-identification strategy described in column strategy. The following strategy types are
   * valid:
   * <p>
   * <ul>
   * <li>{@code "Identity"} - For Strings. Return the same column.</li>
   * <li>{@code "Hash"} - For Strings. Puts the column through SHA512</li>
   * <li>{@code "PBEKeyHash"} - Puts the columns through the javax.crypto PBEKeySpec hash</li>
   * <li>{@code "Drop"} - For all types. Removes the column from the DataFrame.</li>
   * <li>{@code "Duplicate"} - For all types. Creates a new, identical column.</li>
   * <li>{@code "Zip3Digit"} - For Strings. Scrubs away zip information.</li>
   * <li>{@code "Year"} - For dates. Retains only year and days since altered birth date.</li>
   * <li>{@code "Age"} - for numbers. Returns the min(given, 90). </li>
   * <li>{@code "MaxYear"} - for numbers. Returns max(given, current year - 90)</li>
   * <li>{@code "RandomFirstName"} - for any. Returns a random first name</li>
   * <li>{@code "RandomLastName"} - for any. Returns a random last name</li>
   * <li>{@code "FixedFirstName"} - for any. Returns the string "FirstName"</li>
   * <li>{@code "FixedLastName"} - for any. Returns the string "LastName"</li>
   * </ul>
   * <p>
   * Any other type may, but should not be relied upon to, throw an {@link IllegalArgumentException}
   * when it is given as an argument to this function.
   *
   * @param columnStrategy Strategy to be made into a transformation function
   * @return The transformation described by the column strategy
   */
  public DataFrameTransform createTransform(ColumnStrategy columnStrategy) {
    String columnName = columnStrategy.getColumnName();

    switch (columnStrategy.getType()) {
      case "Identity":
        logger.debug("Created Identity transformer for column " + columnName);
        return new IdentityTransform(columnName);
      case "Hash":
        logger.debug("Created Hash transform for column " + columnName);
        return new HashTransform(columnName, retrieveSalt(columnStrategy.saltKey()));
      case "PBEKeyHash":
        logger.debug("Created PBEKeyHash transform for column " + columnName);
        return new PBEKeySpecHashTransform(columnName, retrieveSalt(columnStrategy.saltKey()));
      case "City":
        logger.debug("Created City transform for column " + columnName);
        return new CityTransform(columnName);
      case "Clear":
        logger.debug("Created ClearColumn transform for column " + columnName);
        return new ClearColumnTransform(columnName);
      case "Drop":
        logger.debug("Created Drop transform for column " + columnName);
        return new DropTransform(columnName);
      case "Duplicate":
        logger.debug("Created Duplicate transform for column " + columnName);
        return new DuplicateTransform(columnName);
      case "Zip3Digit":
        logger.debug("Created Zip3Digit transform for column " + columnName);
        return new Zip3Transform(columnName);
      case "EpochStart":
        logger.debug("Created EpochStart transform for column " + columnName);
        return new EpochStartTransform(columnName);
      case "Year":
        logger.debug("Created Year transform for column " + columnName);
        return new DateScrubTransform(columnName);
      case "Age":
        logger.debug("Created Age transform for column " + columnName);
        return new AgeTransform(columnName);
      case "DateEpoch":
        logger.debug("Created DateEpoch transform for column " + columnName);
        return new DateEpochTransform(columnName, columnStrategy.dateEpoch());
      case "DateTimeDay":
        logger.debug("Created DateTimeDay transform for column " + columnName);
        return new DateTimeDayTransform(columnName);
      case "DateDay":
        logger.debug("Created DateDay transform for column " + columnName);
        return new DateDayTransform(columnName);
      case "DateShift":
        logger.debug("Created DateShift transform for column " + columnName);
        return new OncoEMRDateShiftTransform(columnName);
      case "DateTimeShift":
        logger.debug("Created DateTimeShift transform for column " + columnName);
        return new OncoEMRDateTimeShiftTransform(columnName);
      case "RandomFirstName":
        logger.debug("Created RandomFirstName transform for column "
          + columnName);
        return new FirstNameTransform(columnName);
      case "RandomLastName":
        logger.debug("Created RandomLastName transform for column "
          + columnName);
        return new LastNameTransform(columnName);
      case "oncoemrFirstName":
        logger.debug("Created oncoemrFirstName transform for column "
            + columnName);
        return new OncoEMRNameTransform(columnName);
      case "FixedFirstName":
        logger.debug("Created FixedFirstName transform for column " + columnName);
        return new FixedFirstNameTransform(columnName);
      case "FixedLastName":
        logger.debug("Created FixedLastName transform for column " + columnName);
        return new FixedLastNameTransform(columnName);
      case "Ethnicity":
        logger.debug("Created Ethnicity transform for column " + columnName);
        return new EthnicityTransform(columnName);
      case "MaxYear":
        logger.debug("Created MaxYear transform for column " + columnName);
        return new MaxYearTransform(columnName);
      default:
        throw new IllegalArgumentException("Illegal columnStrategy type: "
            + columnStrategy.getType());
    }
  }

  /**
   * Retrieves the salt for the given column. If no such salt exists, generates it and stores it
   * for later use.
   *
   * @param columnName The column which needs salt.
   * @return The salt for the column.
   */
  private String retrieveSalt(String columnName) {
    Objects.requireNonNull(columnName);

    try {
      File f = new File("saltConfigs.json");
      if (f.createNewFile()) logger.info("Creating Salt configuration file.");

      if (map.containsKey(columnName)) {
        return map.get(columnName);
      } else {
        Random r = new SecureRandom();
        byte[] bytes = new byte[128];
        r.nextBytes(bytes);

        String val = toHex(bytes);
        logger.debug(String.format("Added new salt key/value pair: [%s, %s]", columnName, val));
        map.put(columnName, val);
        Gson gson = new Gson();
        String fileContents = gson.toJson(map);
        Files.write(fileContents.getBytes(), f);
        return val;
      }
    } catch (IOException e) {
      e.printStackTrace();
      throw new IllegalStateException("The salt configuration file was not found.");
    }
  }

  /**
   * Registers the needed functions to operate over columns.
   *
   * @param sqlContext The Java SQL context.
   */
  private void registerTransforms(SQLContext sqlContext) {
    Zip3Transform.register(sqlContext);
    CityTransform.register(sqlContext);
    FirstNameTransform.register(sqlContext);
    OncoEMRNameTransform.register(sqlContext);
    LastNameTransform.register(sqlContext);
    HashTransform.register(sqlContext);
    EthnicityTransform.register(sqlContext);
    DateScrubTransform.register(sqlContext);
    DateTimeDayTransform.register(sqlContext);
    DateDayTransform.register(sqlContext);
    OncoEMRDateShiftTransform.register(sqlContext);
    OncoEMRDateTimeShiftTransform.register(sqlContext);
    IdentityTransform.register(sqlContext);
    EpochStartTransform.register(sqlContext);
    PBEKeySpecHashTransform.register(sqlContext);
  }
}
