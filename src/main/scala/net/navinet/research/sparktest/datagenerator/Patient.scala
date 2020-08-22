package net.navinet.research.sparktest.datagenerator

import java.sql.Date

/**
  * Created by pteixeira on 7/8/2016.
  */
private[datagenerator] case class Patient(
                                           patient_id: Long,
                                           patient_mrn: String,
                                           first_name: String,
                                           last_name: String,
                                           dob: Date,
                                           gender: String,
                                           race: String,
                                           zip: String
                                         )
