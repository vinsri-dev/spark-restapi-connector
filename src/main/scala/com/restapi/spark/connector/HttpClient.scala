package com.restapi.spark.connector

import scalaj.http.{Http, HttpResponse}

class HttpClient(url: String) {

  var http = Http(url)

  def setTimeout(timeoutMs: Int): HttpClient = {
    http = http.timeout(connTimeoutMs = timeoutMs, readTimeoutMs = timeoutMs)
    this
  }

  def withAuthHeader(authorizationHeader: String): HttpClient = {
    http = http.header("Authorization", authorizationHeader)
    this
  }

  def withHeaders(headers: String): HttpClient = {
    val requestHeaders = headers
      .split(";")
      .map(_.trim().split(":"))
      .map { case Array(k, v) => (k, v) }
      .toMap
    requestHeaders.foreach { header =>
            http = http.header(header._1.trim, header._2.trim)
    }
    this
  }

  def post(data: String): HttpClient = {
    http = http.postData(data)
    this
  }

  var httpResponse: HttpResponse[String] = null

  def execute: HttpClient = {
    httpResponse = http.asString
    this
  }

  def httpResponseStatusCode: String = {
    httpResponse.code.toString
  }

  def httpResponseBody: String = {
    httpResponse.body
  }
}