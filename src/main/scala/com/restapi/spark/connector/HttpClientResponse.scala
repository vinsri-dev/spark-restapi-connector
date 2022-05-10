package com.restapi.spark.connector

/**
 * Http Client response class. Object of this class is created with response from http request
 * @param RequestUrl
 * @param httpStatusCode
 * @param responseBody
 * @param startTimeUTC
 * @param durationMs
 * @param retryCount
 * @param hostName
 */
case class HttpClientResponse(RequestUrl :String, httpStatusCode:String, responseBody:String, startTimeUTC:String, durationMs:Long, retryCount:Int, hostName:String)