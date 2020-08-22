package net.navinet.research.spark.cclf.parser;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.log4j.Logger;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.InsertableRelation;
import org.apache.spark.sql.sources.TableScan;
import org.apache.spark.sql.types.StructType;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;

/**
 * Main class for the creation of a DataFrame from the contents of a fixed-width file. Required as
 * a part of parsing a fixed-width file into a format usable by Apache Spark.
 */
final class FixedWidthRelation extends BaseRelation
    implements TableScan, InsertableRelation {
  private static final Logger logger = Logger.getLogger(FixedWidthRelation.class);

  private transient final SQLContext sqlContext;
  private final RDD<String> baseRdd;
  private final String path;
  private final FixedWidthParserHelper fileParser;


  public FixedWidthRelation(SQLContext sqlContext, RDD<String> baseRdd,
                            String path, FormatLine[] schemaFile) {
    this.sqlContext = sqlContext;
    this.baseRdd = baseRdd;
    this.path = path;
    this.fileParser = new FixedWidthParserHelper(schemaFile);
  }

  @Override
  public SQLContext sqlContext() {
    return this.sqlContext;
  }

  @Override
  public StructType schema() {
    return this.fileParser.schema();
  }

  @Override
  public String toString() {
    return String.format("CCLFRelation(%s)", this.path);
  }

  @Override
  public void insert(DataFrame data, boolean overwrite) {
    throw new NotImplementedException("Not implemented yet");
  }

  @Override
  public RDD<Row> buildScan() {
    ClassTag<Row> classTag =
        ClassTag$.MODULE$.apply(Row.class);

    logger.trace("Creating RDD of rows from contents of fixed_width file");
      return baseRdd.map(new RowMapper(this.fileParser), classTag);
  }

  /**
   * Wrapper around what amounts to a lambda. The reason for this is that it needs to explicitly be
   * serialiazable.
   */
  private static final class RowMapper
      extends AbstractFunction1<String, Row> implements Serializable {
    private final FixedWidthParserHelper parser;

    RowMapper(FixedWidthParserHelper parser) {
      this.parser = parser;
    }

    @Override
    public Row apply(String v1) {
      return this.parser.parseLine(v1);
    }
  }
}
