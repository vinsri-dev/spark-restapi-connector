package com.restapi.spark.connector

import java.util.{Date, TimeZone}

/**
 * Date Helper Object
 */

object DateHelper {

  /**
   * Returing the time diff between given data objects in Seconds
   * @param from
   * @param to
   * @return
   */
  def getDurationInSeconds(from: Date,
                           to: Date): Long = {
    getDurationInMilliSeconds(from, to) / 1000
  }

  /**
   * Returing the time diff between given data objects in Milliseconds
   * @param from
   * @param to
   * @return
   */
  def getDurationInMilliSeconds(from: Date,
                                to: Date): Long = {
    val duration: Long = (to.getTime() - from.getTime())
    duration
  }

  /**
   * Formatting given date to given format
   * @param date
   * @param format
   * @return
   */
  def format(date: Date,
             format:String): String = {
    import java.text.SimpleDateFormat
    val formatter = new SimpleDateFormat(format)
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    formatter.format(date)
  }

}
