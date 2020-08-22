package net.navinet.research.deidentification.deidentify;

public final class MockColumnStrategy implements ColumnStrategy {
  private final String columnName;
  private final String type;
  private String saltKey;
  private String dateEpoch;

  public MockColumnStrategy(String columnName, String type) {
    this.columnName = columnName;
    this.type = type;
  }

  @Override
  public String getColumnName() {
    return this.columnName;
  }

  @Override
  public String getType() {
    return this.type;
  }

  @Override
  public String saltKey() {
    return this.saltKey;
  }

  public MockColumnStrategy saltKey(String saltKey) {
    this.saltKey = saltKey;
    return this;
  }

  @Override
  public String dateEpoch() {
    return this.dateEpoch;
  }

  public MockColumnStrategy dateEpoch(String dateEpoch) {
    this.dateEpoch = dateEpoch;
    return this;
  }
}
