package com.restapi.spark.connector

import scalaj.http.{Base64, Http}

import scala.util.parsing.json.JSON

/**
 * Rest Api client implementation
 * @param config
 */
class RestClient(private val config: Config
                ) extends Serializable {


  /**
   * Making Rest Api call based on given details
   * @param verb
   * @param uri
   * @param data
   * @param accessToken
   * @param correlationId
   * @param headers
   * @return
   */
  def call(verb: String,
           uri: String,
           data: String,
           accessToken: String,
           correlationId: String,
           headers: String): HttpClientResponse = {

    //Making appropriate http client call based on verb
    verb match {
      case "GET" => call(uri, accessToken, correlationId, headers,httpClient=>httpClient)
      case "POST" => call(uri, accessToken, correlationId, headers, httpClient => httpClient.post(data))
      case "PUT" => call(uri, accessToken, correlationId, headers, httpClient => httpClient.put(data))
    }

  }


  /**
   * Wrapper method to make rest api call with required parameters
   * @param uri
   * @param accessToken
   * @param correlationId
   * @param headers
   * @param httpClientHandler
   * @return
   */
  def call(uri: String,
           accessToken: String,
           correlationId: String,
           headers: String,
           httpClientHandler:(HttpClient=>HttpClient)): HttpClientResponse = {

    val restApiAccessToken = accessToken

    //Reading http client timeout in seconds if sent to connector otherwise default value is considered
    val timeoutMs = config("restcall_timeoutsecs", "60").toString.toInt * 1000

    //Reading http client failure max retries if sent to connector otherwise default value is considered
    val maxRetries = config("restcall_maxretries", "2").toString.toInt

    //Reading http client failure retry interval if sent to connector otherwise default value is considered
    val failureRetryInternalMs = config("restcall_failureretryintervalsecs", "5").toString.toInt * 1000

    //Reading http response status codes to consider as success call. if not specified default value is considered
    val successHttpStatusCodes = config("restcall_succesfulhttpstatuscodes", "200").toString.split(",").map(a => a.toInt).toList

    //Reading whether to fail finally even after retries. if not specified default value is considered
    val failAfterMaxRetries = config("restcall_failaftermaxretries", "true").toString.toBoolean

    //Calling the Http Client fluent Api
    val restResponse =
      httpClientHandler(
      new HttpClient(uri)
      .setCorrelationId(correlationId)
      .withHeaders(headers)
      .setTimeout(timeoutMs)
      .withAuthHeader(restApiAccessToken)
      )
      .executeWithRetry(successHttpStatusCodes, maxRetries, failureRetryInternalMs, failAfterMaxRetries)

    //Returning Http Client Response
    restResponse

  }


  /**
   * Returns Http Client Auth header token based on properties sent to connector
   * @return
   */
  def getRestAPIAuthAccessToken: String = {

    //Identifying the authentication type for the http client Calls. If not specified, considering default value.
    val authenticationType = config("RestCall_AuthenticationType", "BEARER").toUpperCase()

    val accessToken: String = authenticationType match {

      case "BEARER" => getBearerAuthToken()

      case "BASIC" => getBasicAuthToken()

      case _ => throw new Exception(s"Unknown 'RestCall_AuthenticationType' : '$authenticationType' is provided. 'RestCall_AuthenticationType' can be either 'BASIC' or 'BEARER'")
    }
    accessToken

  }

  /**
   * Returns Bearer auth token based on input values from connector properties
   * @return
   */
  def getBearerAuthToken(): String = {

    //Reading Token Request Uri from connector properties
    val tokenUri = config("BearerAuth_RequestUri").toString

    //Reading Token Request headers from connector properties
    val headers = config("BearerAuth_RequestHeaders", "").toString

    //Reading Token Request verb from connector properties otherwise default is considered
    val verb = config("BearerAuth_RequestVerb", "POST").toString.trim.toUpperCase()

    var bearerToken = ""

    //Based on verb making appropriate call to Token Server
    verb match {

      case "POST" => {

        //Reading the token request body from connector properties
        val body = config("BearerAuth_RequestBody").toString

        //Making Http Client Fluent Api call to token server
        val tokenResponse = new HttpClient(tokenUri)
                                .setTimeout(30000)
                                .withHeaders(headers)
                                .post(body)
                                .executeWithRetry(List(200),
                                        0,
                                    0,
                                   true)


        //Parsing response from token server
        val responseMap = JSON.parseFull(tokenResponse.responseBody).get.asInstanceOf[Map[String, String]]

        val accessToken = responseMap("access_token")

        //Forming bearer token
        bearerToken = "Bearer " + accessToken

      }

    }
    bearerToken

  }

  /**
   * Returns Basic auth token based on input values from connector properties
   * @return
   */
  def getBasicAuthToken(): String = {

    //Reading Basic authentication username and password from connector properties
    val userName = config("BasicAuth_UserName").toString
    val password = config("BasicAuth_Password").toString

    "Basic " ++ Base64.encode((userName ++ ":" ++ password).getBytes("UTF-8"))

  }
}
