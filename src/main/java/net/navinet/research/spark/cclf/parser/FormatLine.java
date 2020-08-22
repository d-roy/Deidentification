package net.navinet.research.spark.cclf.parser;

import java.io.Serializable;

/**
 * The reason behind this is that I could
 * not get a reference to the file that contained the schema (actually a {@link java.io.Reader})
 * to be passed around correctly; I have a suspicion that the file itself wasn't being passed
 * around from the master to the workers in the manner that I expected. In any case, the solution
 * that I came up with was to simply read it before performing the job, and sort out everything
 * else later.
 */
interface FormatLine extends Serializable {
  String getName();

  int getLength();

  String getFormat();
}
