package net.navinet.research.deidentification.deidentify;

/**
 * The de-identification strategy to be used on a column. Many of the getters on this class return
 * nullable values; if a getter returns null, then that field was likely not relevant to the column
 * strategy in question.
 */
public interface ColumnStrategy {
  /**
   * Simple getter for name of the column to be transformed.
   *
   * @return the columnName of this instance of ColumnStrategy
   */
  String getColumnName();

  /**
   * Simple getter for type of the transformation to be put in place.
   *
   * @return the type of this instance of ColumnStrategy.
   */
  String getType();

  /**
   * Key used to look up salt in salt-configuration dictionary. Useful only in the case when
   * the type of this object is a hash.
   *
   * @return (Nullable) Dictionary lookup key of the salt to be used on this column
   */
  String saltKey();

  /**
   * Column name to be used when doing a date-epoch strategy for de-identification. The column
   * must come from the same table as this object's column at the time when the de-identification
   * is made. Useful only when the type of this object is {@code "dateEpoch"}
   *
   * @return (Nullable) Column name to be used as the origin when doing a date-epoch strategy
   */
  String dateEpoch();
}
