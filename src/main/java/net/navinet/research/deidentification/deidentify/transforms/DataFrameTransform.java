package net.navinet.research.deidentification.deidentify.transforms;

import org.apache.spark.sql.DataFrame;

/**
 * Represents a user-defined function from DataFrames to DataFrames. This
 * can include a mapping on a single column, such as hashing the values in a
 * column or dropping a column, or operations on a multiple columns, such as
 * generating new columns, perhaps because of NLP enrichment.
 */
public interface DataFrameTransform {
  /**
   * Apply the mapping represented by this object to the given {@link DataFrame}.
   *
   * @param input {@code DataFrame} to be transformed by this mapping
   * @return {@code DataFrame} after applying the mapping
   */
  DataFrame transform(DataFrame input);
}
