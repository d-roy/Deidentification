package net.navinet.research.deidentification.schema;

import net.navinet.research.deidentification.deidentify.ColumnStrategy;

import java.util.List;

/**
 * Description of the process of reading from a database. This is only a thin wrapper around
 * the actual format of the de-id schema file for databases.
 */
final class FrameConfigurationImpl implements FrameConfiguration {
  private final String tableName;
  private final String partitioningColumn;
  private final List<ColumnStrategy> columnStrategyList;

  FrameConfigurationImpl(String tableName, String partitioningColumn,
                         List<ColumnStrategy> columnStrategies) {
    this.tableName = tableName;
    this.partitioningColumn = partitioningColumn;
    this.columnStrategyList = columnStrategies;
  }

  @Override
  public String toString() {
    return "FrameConfiguration{" +
      "name='" + tableName + '\'' +
      ", partitioningColumn='" + partitioningColumn + '\'' +
      '}';
  }

  @Override
  public String name() {
    return tableName;
  }

  @Override
  public String partitioningColumn() {
    return partitioningColumn;
  }

  @Override
  public List<ColumnStrategy> columnStrategies() {
    return columnStrategyList;
  }
}
