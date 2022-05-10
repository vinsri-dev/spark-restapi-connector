package com.restapi.spark.connector

import com.restapi.spark.connector.ExceptionHelper.getExceptionDetailMessage
import scalaj.http.Http

import scala.util.control.Breaks.{break, breakable}

/**
 * Http Client Fluent Class
 * @param url
 */

class HttpClient(url: String) {

  var correlationId:String=null

  var http = Http(url)

  /**
   * Set http request timeout in milliseconds
   * @param timeoutMs
   * @return
   */
  def setTimeout(timeoutMs: Int): HttpClient = {
    http = http.timeout(connTimeoutMs = timeoutMs, readTimeoutMs = timeoutMs)
    this
  }

  /**
   * Set correlation Id for current http request
   * @param correlationId
   * @return
   */
  def setCorrelationId(correlationId: String): HttpClient = {
    this.correlationId=correlationId
    this
  }

  /**
   * Set authorization header for current http request
   * @param authorizationHeader
   * @return
   */
  def withAuthHeader(authorizationHeader: String): HttpClient = {
    http = http.header("Authorization", authorizationHeader)
    this
  }

  /**
   * Set headers for current http request
   * @param headers
   * @return
   */
  def withHeaders(headers: String): HttpClient = {

    if(!headers.trim.isEmpty) {
      //Preparing the Map of headers to be passed in http request
      val requestHeaders = headers
        .split(";")
        .map(_.trim().split(":"))
        .map { case Array(k, v) => (k, v) }
        .toMap

      //Adding each header to http request headers
      requestHeaders.foreach { header =>
        http = http.header(header._1.trim, header._2.trim)
      }
    }
    this

  }

  /**
   * Call POST in current http request
   * @param data
   * @return
   */
  def post(data: String): HttpClient = {
    if (data != null)
      http = http.postData(data)
    this
  }

  /**
   * Call PUT in current http request
   * @param data
   * @return
   */
  def put(data: String): HttpClient = {
    if (data != null)
      http = http.put(data)
    this
  }

  /**
   * Execute Http Request with retries based on response status code
   * @param successHttpStatusCodes
   * @param maxRetries
   * @param retryIntervalInMs
   * @param failAfterMaxRetries
   * @return
   */
  def executeWithRetry(successHttpStatusCodes: List[Int],
                       maxRetries: Int,
                       retryIntervalInMs: Int,
                       failAfterMaxRetries: Boolean): HttpClientResponse = {
    var retryCount = 0
    var restCallFailureException: Exception = null
    var startDateTime: java.util.Date = new java.util.Date()
    var endDateTime: java.util.Date = new java.util.Date()
    val timeStampFormat = "yyyy-MM-dd HH:mm:ss.SSS"

    //Default Http Client response object
    var httpClientResponse: HttpClientResponse = new HttpClientResponse(url,
      "unset",
      "unset",
      "unset",
      -1,
      -1,
      "")

    breakable {

      //Retrying till successful http request call or till max retries
      while (retryCount == 0 || retryCount < maxRetries) {

        try {

          startDateTime = new java.util.Date()
          val httpResponse = http.asString
          endDateTime = new java.util.Date()

          val httpStatusCode = httpResponse.code
          val response = httpResponse.body
          restCallFailureException = null

          //Prepring Http Client Response object based on received Http Response Status Code and Body
          httpClientResponse = new HttpClientResponse(url,
            httpResponse.code.toString,
            httpResponse.body,
            DateHelper.format(startDateTime, timeStampFormat),
            DateHelper.getDurationInMilliSeconds(startDateTime, endDateTime),
            retryCount,
            getHostName)

          //Breaking look, if the http status code is considered to be successful
          if (successHttpStatusCodes.contains(httpStatusCode)) {
            break
          }
          else {
            throw new Exception("Not a successful Http Status Code. httpStatusCode : " + httpStatusCode + ", " + response)
          }

        }
        catch {

          case e: Exception => {

            restCallFailureException = e
            endDateTime = new java.util.Date()

            //Prepring Http Client Response object for exception scenario
            httpClientResponse = new HttpClientResponse(url,
              httpClientResponse != null && httpClientResponse.httpStatusCode != "unset"
              match {
                case true => httpClientResponse.httpStatusCode
                case false => "Exception"
              },
              getExceptionDetailMessage(e),
              DateHelper.format(startDateTime, timeStampFormat),
              DateHelper.getDurationInMilliSeconds(startDateTime, endDateTime),
              retryCount,
              getHostName)

            //Retrying with sleep if within Max retry limit
            if (retryCount < maxRetries) {
              retryCount = retryCount + 1
              Thread.sleep(retryIntervalInMs)
            }
            else
              break
          }

        }

      }

    }

    //Incase if expected to fail even after max retries, raising exception
    if (failAfterMaxRetries && restCallFailureException != null) {
      throw new Exception(s"Http Client call failed for  Url : $url.", restCallFailureException)
    }

    httpClientResponse
  }

  /**
   * Retruing the host name, since there will be parallel http calls for each row in RDD
   * capturing the worker node name as host name
   * @return
   */
  def getHostName: String = {
    val addr: java.net.InetAddress = java.net.InetAddress.getLocalHost
    addr.getHostName
  }

}