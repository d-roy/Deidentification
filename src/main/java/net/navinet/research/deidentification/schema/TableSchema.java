package net.navinet.research.deidentification.schema;

import net.navinet.research.deidentification.deidentify.ColumnStrategy;
import net.navinet.research.deidentification.deidentify.transforms.DataFrameTransform;
import net.navinet.research.deidentification.deidentify.transforms.DataFrameTransformFactory;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrame;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Represents the schema configuration format for a single element (eg, a table) from a data
 * source to be de-identified.
 */
public final class TableSchema {
  private static final Logger logger = LogManager.getLogger(TableSchema.class);

  private final List<ColumnStrategy> columnStrategies;
  private final DataFrameTransformFactory transformFactory;

  public TableSchema(List<ColumnStrategy> columnStrategies, DataFrameTransformFactory factory) {
    Objects.requireNonNull(columnStrategies);
    Objects.requireNonNull(factory);

    this.columnStrategies = columnStrategies;
    this.transformFactory = factory;
  }

  public DataFrame deidentify(DataFrame input) {
    Objects.requireNonNull(input);

    DataFrame dataFrame = input;

    List<String> columns = new ArrayList<>();
    for (ColumnStrategy strategy : this.columnStrategies) {
      columns.add(strategy.getColumnName().toLowerCase());
    }

    for (ColumnStrategy strategy : this.columnStrategies) {
      DataFrameTransform transform = this.transformFactory.createTransform(strategy);
      dataFrame = transform.transform(dataFrame);
    }

    // For sources other than database tables, explicitly drop columns which were not
    // given column transforms
    for (String columnName : dataFrame.columns()) {
      if (!columns.contains(columnName.toLowerCase())) {
        logger.warn("No column strategy given for column " + columnName + ". Dropping.");
        dataFrame = dataFrame.drop(columnName);
      }
    }

    return dataFrame;
  }
}
