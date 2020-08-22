package net.navinet.research.spark.cclf.parser;

class MockFormatLine implements FormatLine {
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
