package net.navinet.research.sparktest.datagenerator

import java.sql.Timestamp

/**
  * Created by pteixeira on 7/8/2016.
  */
private[datagenerator] case class Heartrate(
                                        patient_id: Long,
                                        timestamp: Timestamp,
                                        heartrate: Long
                                      )
