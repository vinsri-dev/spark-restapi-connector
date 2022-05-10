package com.restapi.spark.connector

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{DataSourceRegister, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

/**
 * Entry point for Rest API Connector
 */

class DefaultSource extends DataSourceRegister with SchemaRelationProvider {

  //connector can be called using below short name
  override def shortName(): String = "rest"

  /**
   * Override implementation to return appropriate relation which makes rest calls based
   * on configuration settings and data in rdd rows
   * @param sqlContext
   * @param parameters
   * @param schema
   * @return
   */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): RestApiRelation = {

    new RestApiRelation(getConfig(sqlContext, parameters), schema)(sqlContext.sparkSession)

  }

  /**
   * Returns config object based on key value pair sent while calling connector
   * Config properties are also read from configuration file if specified in the key value pair with appropriate path
   * @param sqlContext
   * @param parameters
   * @return
   */
  def getConfig(sqlContext: SQLContext,
                parameters: Map[String, String]): Config = {

    //Prepring config object
    var config = Config(sqlContext.sparkSession, parameters)

    val restClient = new RestClient(config)

    //Fetching access token to make rest calls
    val accessToken = restClient.getRestAPIAuthAccessToken

    config = Config(sqlContext.sparkSession, Map("RestAPIAuthAccessToken" -> accessToken) ++ parameters)

    config
  }

}
