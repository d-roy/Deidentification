package net.navinet.research.spark.cclf.parser;

import com.google.common.primitives.Ints;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

/**
 * Wrapper around line-by-line parsing a fixed-width file format.
 */
final class FixedWidthParserHelper implements Serializable {
  private static final Logger logger = Logger.getLogger(FixedWidthParserHelper.class);

  private final FormatLine[] schemaFile;
  private final StructType schema;
  private final int[] partitions;

  /**
   * Create a parser helper from the schema document for a CCLF table
   *
   * @param schemaFile Parsed form of the JSON CCLF schema dockument
   */
  FixedWidthParserHelper(FormatLine[] schemaFile) {
    this.schemaFile = schemaFile;
    this.schema = this.schema(schemaFile);
    this.partitions = this.partitions(schemaFile);
  }

  /**
   * Convert the data types indicated in the JSON CCLF schema (eg, {@code "X"},
   * {@code "YYYY-MM-DD"}) into Apache Spark's {@code StructField} elements, which contain the
   * data type of the column (eg, {@code StringType} or {@code DateType}) and the name of the
   * column. This is required to create a DataFrame and to perform the column-wise DataFrame
   * operations.
   *
   * @return The Schema described by the JSON CCLF schema
   */
  StructType schema() {
    return this.schema;
  }

  /**
   * Called in the constructor to cache the StructType, so that the call to the public
   * version of this method doesn't need to recompute every time.
   *
   * @param schemaFile Parsed schema file to read from
   * @return Type information for the row
   */
  private StructType schema(FormatLine[] schemaFile) {
    StructField[] fields = new StructField[schemaFile.length];

    for (int i = 0; i < schemaFile.length; i += 1) {
      fields[i] = new StructField(
        schemaFile[i].getName(),
        this.typeFromFormat(schemaFile[i].getFormat()),
        false, Metadata.empty());
    }

    return new StructType(fields);
  }

  /**
   * Take a line of a CCLF file and transform it into a row which will eventually go into a
   * DataFrame. This involves breaking the line into tokens and associating each token with a
   * data type.
   *
   * @param line Line of CCLF file to be parsed as Row
   * @return Row of eventual DataFrame
   */
  Row parseLine(String line) {
    String[] tokens = this.tokenize(line);
    StructField[] schemaFields = this.schema.fields();
    SimpleDateFormat dashFormat = new SimpleDateFormat("yyyy-MM-dd");
    SimpleDateFormat slashFormat = new SimpleDateFormat("M/d/yyyy");

    Object[] row = new Object[schemaFields.length];

    for (int i = 0; i < schemaFields.length; i += 1) {
      DataType tokenType = schemaFields[i].dataType();
      if (tokenType.sameType(DataTypes.DateType)) {
        try {
          if (tokens[i].matches("\\s*")) {
            row[i] = null;
          } else if (tokens[i].matches("\\d\\d\\d\\d-\\d\\d-\\d\\d")) {
            row[i] = new Date(dashFormat.parse(tokens[i]).getTime());
          } else if (tokens[i].matches("\\d{1,2}/\\d{1,2}/\\d{2,4}")) {
            row[i] = new Date(slashFormat.parse(tokens[i]).getTime());
          } else {
            logger.error("Failed to parse text " + tokens[i] + " as a date");
            throw new IllegalArgumentException("Failed to parse text " + tokens[i] + " as a date.");
          }
        } catch (ParseException e) {
          logger.error("Failed to parse text " + tokens[i] + " as a date");
          throw new IllegalArgumentException("Failed to parse text " + tokens[i] + " as a date.");
        }
      } else if (tokenType.sameType(DataTypes.StringType)) {
        row[i] = tokens[i];
      } else if (tokenType.sameType(DataTypes.LongType)) {
        try {
               row[i] = Long.parseLong(tokens[i]);
          } catch(NumberFormatException e) {
            row[i] = Long.parseLong("0");
            logger.error("Failed to convert "+tokens[i]+" to long");
          }
      } else if (tokenType.sameType(DataTypes.DoubleType)) {
        if (tokens[i].equals("Z")) {
          row[i] = 0.0;
        } else {
          row[i] = Double.parseDouble(tokens[i]);
        }
      } else {
        logger.warn("Encountered a token type which was not expected: " + tokenType.typeName());
        row[i] = tokens[i];
      }
    }

    return RowFactory.create(row);
  }

  /**
   * Transform a single row from a DataFrame into a text line that can be written back in CCLF
   * format
   *
   * @param row Row from a DataFrame
   * @return String which can be serialized back into CCLF format
   */
  String format(Row row) {
    String[] output = new String[schemaFile.length];

    for (int i = 0; i < output.length; i++) {
      output[i] = this.convertElementToString(row, this.schemaFile[i]);
    }

    return StringUtils.join(output, "");
  }

  /**
   * Pass-through to reach {@link FixedWidthParser#flatParse(Iterable, String)}
   *
   * @param line Line from CCLF file
   * @return Tokenized version of that line
   */
  private String[] tokenize(String line) {
    List<String> tokens = FixedWidthParser.flatParse(Ints.asList(this.partitions), line);

    String[] strippedTokens = new String[tokens.size()];
    for (int i = 0; i < tokens.size(); i++) {
      strippedTokens[i] = tokens.get(i).trim();
    }


    return strippedTokens;
  }

  /**
   * @param schemaFile Parsed schema file to read from
   * @return List of all break points in the fixed-width file
   */
  private int[] partitions(FormatLine[] schemaFile) {
    int[] partitions = new int[schemaFile.length];

    for (int i = 0; i < schemaFile.length; i += 1) {
      partitions[i] = schemaFile[i].getLength();
    }

    return partitions;
  }


  /**
   * Wrapper around a switch statement. In the event that it hits an unexpected data type in the
   * schema file, it will default to string type and log a warning.
   *
   * @param type String descripion of type from CCLF schema file
   * @return Apache Spark's DataType classification
   */
  private DataType typeFromFormat(String type) {
    if (type.equals("X")) {
      return DataTypes.StringType;
    } else if (type.equals("9")) {
      return DataTypes.LongType;
    } else if (type.matches("-9.9+")) {
      return DataTypes.DoubleType;
    } else if (type.equals("YYYY-MM-DD")) {
      return DataTypes.DateType;
    } else {
      logger.error("Unexpected data format type: " + type);
      throw new IllegalArgumentException("Unexpected data type: " + type);
    }
  }

  private String dateToString(Date value, int length) {
      if(value == null) {
          return StringUtils.rightPad("", length);
      }
      Format formatter = new SimpleDateFormat("MM/dd/yyyy");
      return formatter.format(value);
  }

  private String doubleToString(double value, int length) {
      //TOOD: If we have double strategy this can produce more than fixed width length
      String s = Double.toString(value);
      return StringUtils.rightPad(s , length);
    //return String.format("%" + length + ".2f", value);
  }

  private String intToString(long value, int length) {
      String s = Long.toString(value);
      return StringUtils.rightPad(s.substring(0, Math.min(s.length(), length)) , length);
  }

  private String stringToString(String value, int length) {
      return StringUtils.rightPad(value.substring(0, Math.min(value.length(), length)), length);
  }

  private String convertElementToString(Row r, FormatLine schema) {
    String name = schema.getName();
    DataType tokenType = this.typeFromFormat(schema.getFormat());

    // Can't do a switch here, unfortunately
    if (tokenType.sameType(DataTypes.DateType)) {
      Date value;
      try {
        Object value3 = r.getAs(name);

        if(value3 == null) {
            value = null;
        }
        else if(value3 instanceof Date) {
          value = (Date)value3;
        }
        else if(value3 instanceof Timestamp) {
          value = new Date(((Timestamp)value3).getTime());
        }
        else {
          throw new IllegalArgumentException("value not type of Date/Timestamp");
        }
      } catch (IllegalArgumentException e) {
        value = null;
      }
      return this.dateToString(value, schema.getLength());
    } else if (tokenType.sameType(DataTypes.StringType)) {
      String value;
      try {
        value = r.getAs(name);
      } catch (IllegalArgumentException e) {
        value = "0";
      }
      return this.stringToString(value, schema.getLength());
    } else if (tokenType.sameType(DataTypes.LongType)) {
      long value;
      try {
         Object value1 = r.getAs(name);
         value = Long.parseLong(value1.toString());
      } catch (IllegalArgumentException e) {
        value = 0;
      }
      return this.intToString(value, schema.getLength());
    } else if (tokenType.sameType(DataTypes.DoubleType)) {
      double value;
      try {
        value = r.getAs(name);
      } catch (IllegalArgumentException e) {
        value = 0;
      }
      return this.doubleToString(value, schema.getLength());
    } else {
      logger.error("Encountered a token type which was not expected: " + tokenType.typeName());
      throw new IllegalArgumentException("Encountered bad token type: " + tokenType.typeName());
    }
  }
}
