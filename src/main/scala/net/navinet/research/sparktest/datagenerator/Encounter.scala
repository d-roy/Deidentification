package net.navinet.research.sparktest.datagenerator

import java.sql.Date

/**
  * Created by pteixeira on 7/8/2016.
  */
private[datagenerator] case class Encounter(
                                             patient_id: Long,
                                             physician_id: Long,
                                             location_id: Long,
                                             visit_type: String,
                                             encounter_date: Date,
                                             notes: String
                                           )
