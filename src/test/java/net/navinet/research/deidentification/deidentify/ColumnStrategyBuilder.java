package net.navinet.research.deidentification.deidentify;

import java.util.ArrayList;
import java.util.List;

/**
 * Builder for quickly constructing lists of column strategies
 */
public final class ColumnStrategyBuilder {
  private final List<ColumnStrategy> columnStrategies = new ArrayList<>();

  public ColumnStrategyBuilder add(String columnName, String type) {
    this.columnStrategies.add(new MockColumnStrategy(columnName, type));

    return this;
  }

  public ColumnStrategyBuilder addWithSalt(String columnName, String type, String saltKey) {
    this.columnStrategies.add(new MockColumnStrategy(columnName, type).saltKey(saltKey));

    return this;
  }

  public ColumnStrategyBuilder addWithDateEpoch(String columnName, String type, String dateEpoch) {
    this.columnStrategies.add(new MockColumnStrategy(columnName, type).dateEpoch(dateEpoch));

    return this;
  }

  public List<ColumnStrategy> build() {
    return this.columnStrategies;
  }
}
