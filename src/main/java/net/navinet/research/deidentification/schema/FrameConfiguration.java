package net.navinet.research.deidentification.schema;

import net.navinet.research.deidentification.deidentify.ColumnStrategy;

import java.util.List;

/**
 * Wrapper around accessing a single data frame, as described in the inbound schema
 * configuration file.
 */
public interface FrameConfiguration {
  /**
   * Returns the name of the resource which will go into this {@code DataFrame}. This is derived
   * from the top-level entries in the schema configuration file. It may be, for example, the
   * name of a table to be read from a database, or the name of a file to be loaded. There
   * is no standard interpretation for this field, and it relies entirely on the caller to
   * provide an interpretation, namely by changing this field into a {@code DataFrame}.
   *
   * @return The name of the resource
   */
  String name();

  /**
   * This field is NULLABLE, and should be checked for nullity whenever it is used.
   *
   * @return The name of the column to be equally divided when partitioning.
   */
  String partitioningColumn();

  /**
   * List of the transformations ("column strategies") that are to be applied to the
   * {@code DataFrame} described by this instance. Unlike the {@link this#name()} method, there
   * is a standard interpretation for this field. In particular, the calling code will look
   * something like this:
   *
   * {@code
   *   FrameConfiguration config = new SchemaAccessor().readSingleConfiguration(r);
   *   DataFrame input;
   *   DataFrameTransformFactory transformFactory;
   *
   *   TableSchema tableSchema = new TableSchema(config.columnStrategies(), transformFactory);
   *
   *   DataFrame output = tableSchema.deidentify(input);
   * }
   *
   * @return Transformations to be applied to the {@code DataFrame}.
   */
  List<ColumnStrategy> columnStrategies();
}
