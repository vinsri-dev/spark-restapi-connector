package com.restapi.spark.connector

import play.api.libs.json.{JsObject, Json}

class BasicAuthTests extends BaseTest {

  /**
   * Basic authentication happy scenario
   */
  "Basic : Calling RestApi Default Source with BasicAuth_UserName" should " pass" in {

    val spark2 = Server.sparkSession
    import spark2.implicits._

    //Random basic auth username and password
    val BasicAuth_UserName = s"User_${getRandomString}"
    val basicAuth_P_k_S_e_W = s"Password_${getRandomString}"

    val inputViewName = "TestPostAPI_" + getRandomString

    //Static dataframe with 2 rows.
    //Each row has a url to an external site that returns a valid response if username and password matches in request
    //and basic auth token passed by connector

    val inputDF = Seq(
      (s"https://httpbin.org/basic-auth/$BasicAuth_UserName/$basicAuth_P_k_S_e_W", "correlationid1"),
      (s"https://httpbin.org/basic-auth/$BasicAuth_UserName/$basicAuth_P_k_S_e_W", "correlationid2")
    ).toDF("Url", "CorrelationId")

    inputDF.createOrReplaceTempView(inputViewName)

    val vwName1 = getRandomString

    //Sql using the rest api connector and passing appropriate properties
    val tempView =
      s"""
         CREATE Or replace TEMPORARY VIEW $vwName1
         |(
         |  Url string,
         |  CorrelationId string
         |)
         |USING com.restapi.spark.connector
         |OPTIONS (
         |Table "$inputViewName",
         |CorrelationIdProperty "CorrelationId",
         |ResourceUriProperty "Url",
         |BasicAuth_UserName "$BasicAuth_UserName",
         |BasicAuth_Password "$basicAuth_P_k_S_e_W",
         |RestCall_AuthenticationType "BasiC",
         |RestCall_RequestHeaders "content-type:application/json"
         |)""".stripMargin
    Server.sparkSession.sql(tempView)

    //Parsing the auto added "RestCall_Response" column which is the response from external site
    //as json string

    //Filtering for correlationId1
    val corId1Values=parseFirstRowAndFirstColumnAsJSON(Server.sparkSession.sql(
      s"select RestCall_Response from $vwName1 where CorrelationId = 'correlationid1'"))

    //Filtering for correlationId2
    val corId2Values=parseFirstRowAndFirstColumnAsJSON(Server.sparkSession.sql(
      s"select RestCall_Response from $vwName1 where CorrelationId = 'correlationid2'"))

    //External site returns username in the json response, validating that it has to match with input
    //generated usernames

    val actualUserName1 = corId1Values.get("user").get.replaceAll("\"","")
    assert(actualUserName1.contentEquals(BasicAuth_UserName))

    val actualUserName2= corId2Values.get("user").get.replaceAll("\"","")
    assert(actualUserName2.contentEquals(BasicAuth_UserName))
  }

  /**
   * Basic authentication test case failed scenario with invalid password. Since "RestCall_FailAfterMaxRetries" is set to "true",
   * expecting an exception.
   */
  "Basic : Calling RestApi Default Source with BasicAuth_UserName and wrong password" should " raise error as expected" in {

    val spark2 = Server.sparkSession
    import spark2.implicits._

    //Random basic auth username and password
    val BasicAuth_UserName = s"User_${getRandomString}"
    val basicAuth_P_k_S_e_W = s"Password_${getRandomString}"

    val inputViewName = "TestPostAPI_" + getRandomString

    //Static dataframe with 2 rows.
    //Each row has a url to an external site that returns a valid response if username and password matches in request
    //and basic auth token passed by connector

    val inputDF = Seq(
      (s"https://httpbin.org/basic-auth/$BasicAuth_UserName/wrongpassword", "correlationid1"),
      (s"https://httpbin.org/basic-auth/$BasicAuth_UserName/$basicAuth_P_k_S_e_W", "correlationid2")
    ).toDF("Url", "CorrelationId")

    inputDF.createOrReplaceTempView(inputViewName)

    val vwName1 = getRandomString

    //Sql using the rest api connector and passing appropriate properties
    val tempView =
      s"""
         CREATE Or replace TEMPORARY VIEW $vwName1
         |(
         |  Url string,
         |  CorrelationId string
         |)
         |USING com.restapi.spark.connector
         |OPTIONS (
         |Table "$inputViewName",
         |CorrelationIdProperty "CorrelationId",
         |ResourceUriProperty "Url",
         |BasicAuth_UserName "$BasicAuth_UserName",
         |BasicAuth_Password "$basicAuth_P_k_S_e_W",
         |RestCall_AuthenticationType "BasiC",
         |RestCall_FailAfterMaxRetries "true",
         |RestCall_FailureRetryIntervalSecs "2",
         |RestCall_MaxRetries "2"
         |)""".stripMargin
    Server.sparkSession.sql(tempView)


    val expectedExceptionMsg = "Not a successful Http Status Code. httpStatusCode"
    var actualException:Exception = null

    try {

      //Filtering for correlationId1
      val corId1Values = parseFirstRowAndFirstColumnAsJSON(Server.sparkSession.sql(
        s"select RestCall_Response from $vwName1 where CorrelationId = 'correlationid1'"))

    }
    catch {

      case e:Exception=>{
        actualException = e
      }

    }

    assert(actualException.getMessage.contains("Not a successful Http Status Code. httpStatusCode"))

  }

  /**
   * Basic authentication test case failed scenario with invalid password. Since "RestCall_FailAfterMaxRetries" is set to "false",
   * not expecting an exception but have the row in dataframe with error details.
   */
  "Basic : Calling RestApi Default Source with BasicAuth_UserName and wrong password" should " not raise error but captured as failed" in {

    val spark2 = Server.sparkSession
    import spark2.implicits._

    //Random basic auth username and password
    val BasicAuth_UserName = s"User_${getRandomString}"
    val basicAuth_P_k_S_e_W = s"Password_${getRandomString}"

    val inputViewName = "TestPostAPI_" + getRandomString

    //Static dataframe with 2 rows.
    //Each row has a url to an external site that returns a valid response if username and password matches in request
    //and basic auth token passed by connector

    val inputDF = Seq(
      (s"https://httpbin.org/basic-auth/$BasicAuth_UserName/wrongpassword", "correlationid1"),
      (s"https://httpbin.org/basic-auth/$BasicAuth_UserName/$basicAuth_P_k_S_e_W", "correlationid2")
    ).toDF("Url", "CorrelationId")

    inputDF.createOrReplaceTempView(inputViewName)

    val vwName1 = getRandomString

    //Sql using the rest api connector and passing appropriate properties
    val tempView =
      s"""
         CREATE Or replace TEMPORARY VIEW $vwName1
         |(
         |  Url string,
         |  CorrelationId string
         |)
         |USING com.restapi.spark.connector
         |OPTIONS (
         |Table "$inputViewName",
         |CorrelationIdProperty "CorrelationId",
         |ResourceUriProperty "Url",
         |BasicAuth_UserName "$BasicAuth_UserName",
         |BasicAuth_Password "$basicAuth_P_k_S_e_W",
         |RestCall_AuthenticationType "BasiC",
         |RestCall_FailAfterMaxRetries "false",
         |RestCall_FailureRetryIntervalSecs "2",
         |RestCall_MaxRetries "4"
         |)""".stripMargin
    Server.sparkSession.sql(tempView)

    //Filtering for correlationId1 and reading response
    val response=Server.sparkSession.sql(
      s"select RestCall_Response from $vwName1 where CorrelationId = 'correlationid1' and RestCall_Retry = 3 and RestCall_HttpStatusCode = '401'").first().getString(0)


    assert(response.contains("Not a successful Http Status Code. httpStatusCode : 401"))

  }

}