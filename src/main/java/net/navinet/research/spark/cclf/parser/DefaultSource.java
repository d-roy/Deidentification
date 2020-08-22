package net.navinet.research.spark.cclf.parser;

import com.google.gson.Gson;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.RelationProvider;
import scala.Option;
import scala.collection.immutable.Map;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;

/**
 * Register this package's utilities for parsing fixed-width formats onto the DataFrameReader and
 * DataFrameWriter
 */
public final class DefaultSource implements
    DataSourceRegister, RelationProvider, CreatableRelationProvider {
  private static final Logger logger = Logger.getLogger(DefaultSource.class);

  @Override
  public String shortName() {
    return "cclf";
  }

  @Override
  public BaseRelation createRelation(SQLContext sqlContext, SaveMode mode,
                                     Map<String, String> parameters, DataFrame data) {
    logger.trace("Saving data frame in fixed-width format. Parameters: "
      + parameters + ". SaveMode: " + mode);
    String path = this.checkPath(parameters);
    String schemaPath = this.checkSchema(parameters);

    Path filePath = new Path(path);
    boolean canSave;
    try (FileSystem fileSystem = filePath.getFileSystem(
        sqlContext.sparkContext().hadoopConfiguration())) {

      Reader schema = new InputStreamReader(
          this.getClass()
              .getClassLoader()
              .getResourceAsStream(schemaPath));
      FormatLine[] schemaFile = new Gson()
          .fromJson(schema, FormatLineImpl[].class);
      FixedWidthParserHelper parser = new FixedWidthParserHelper(schemaFile);
      canSave = false;

      if (fileSystem.exists(filePath)) {
        switch (mode) {
          case Append:
            canSave = true;
            throw new NotImplementedException("Append not implemented yet");
            //break;
          case Overwrite:
            fileSystem.delete(filePath, true);
            canSave = true;
            break;
          case ErrorIfExists:
            throw new IllegalStateException(String.format("Path %s already exists",
                filePath.toString()));
          case Ignore:
            canSave = false;
            break;
        }
      } else {
        canSave = true;
      }

      if (canSave) {
        logger.trace("Successfully saved data frame in fixed width format.");
        ClassTag<String> classTag = ClassTag$.MODULE$.apply(String.class);

        data.map(new StringMapper(parser), classTag)
            .saveAsTextFile(path);
      }
    } catch (IOException exception) {
      logger.error(String.format("Failed to save file at path %s", path), exception);
    }

    return this.createRelation(sqlContext, parameters);
  }

  @Override
  public BaseRelation createRelation(final SQLContext sqlContext, Map<String, String> parameters) {
    final String path = this.checkPath(parameters);
    final String schemaPath = this.checkSchema(parameters);

    Reader schema = new InputStreamReader(
        this.getClass()
            .getClassLoader()
            .getResourceAsStream(schemaPath));

    FormatLine[] schemaFile = new Gson()
        .fromJson(schema, FormatLineImpl[].class);

    logger.trace("Creating new FixedWidthRelation.");
    return new FixedWidthRelation(sqlContext, sqlContext.sparkContext().textFile(path, 2),
        path, schemaFile);
  }

  /**
   * Helper function intended to guarantee fail-fast behavior if required options
   * are not set when attempting to load the fixed-width file. In particular,
   * this function will simply return the ath, if present, or throw an exception
   * to crash the application if the path is not set.
   *
   * @param parameters Options set on {@link org.apache.spark.sql.DataFrameReader}
   * @return Path to source file
   * @throws IllegalStateException if path not set
   */
  private String checkPath(Map<String, String> parameters) {
    Option<String> maybePath = parameters.get("path");
    if (maybePath.nonEmpty()) {
      return maybePath.get();
    } else {
      logger.error("Option \"path\" must be specified for fixed-width data.");
      throw new IllegalStateException("Option \"path\" must be specified for fixed-width data.");
    }
  }

  /**
   * Helper function intended to guarantee fail-fast behavior if required options
   * are not set when attempting to load the fixed-width file. In particular,
   * this function will simply return the path to the schema file, if present, or
   * throw an exception to crash the application if the path is not set.
   *
   * @param parameters Options set on the {@link org.apache.spark.sql.DataFrameReader}
   * @return Path to schema file
   * @throws IllegalStateException if schema_path not set
   */
  private String checkSchema(Map<String, String> parameters) {
    Option<String> maybeSchema = parameters.get("schema_path");
    if (maybeSchema.nonEmpty()) {
      return maybeSchema.get();
    } else {
      logger.error("Option \"schema_path\" must be specified for fixed-width data.");
      throw new IllegalStateException(
          "Option \"schema_path\" must be specified for fixed-width data.");
    }
  }

  /**
   * Default implementation of the {@link FormatLine} interface.
   */
  private static final class FormatLineImpl implements FormatLine {
    String name;
    int length;
    String format;

    @Override
    public String getName() {
      return name;
    }

    @Override
    public int getLength() {
      return length;
    }

    @Override
    public String getFormat() {
      return format;
    }
  }

  /**
   * Used when changing Rows back into Strings in the process of writing out to a file.
   */
  private static final class StringMapper
      extends AbstractFunction1<Row, String> implements Serializable {
    private final FixedWidthParserHelper parser;

    StringMapper(FixedWidthParserHelper parser) {
      this.parser = parser;
    }

    @Override
    public String apply(Row v1) {
      return parser.format(v1);
    }
  }
}
