package com.restapi.spark.connector

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

/**
 * Implementation of Rest Api relation
 * @param config
 * @param schemaProvided
 * @param sparkSession
 */
class RestApiRelation (
                        private  val config : Config,
                        schemaProvided: StructType
                      )
                      (@transient val sparkSession: SparkSession )
  extends BaseRelation with TableScan with Serializable{


  val restClient = new RestClient(config)

  //Identifying the column from where to read the "ResourceUrl" value in the Row.
  //If not specified default value is "ResourceUrl".
  val resourceUriProperty = config("ResourceUriProperty", "ResourceUrl").toString

  //Identifying the request headers to be added before making http request for each Row.
  //If not specified default value is considered.
  val restCallRequestHeaders = config("RestCall_RequestHeaders",
    "Accept:application/json;Content-Type:application/json").toString

  //Identifying the verb to be considered for http request for each Row.
  //If not specified default value is considered.
  val restCallVerb = config("RestCall_Verb", "GET").toString

  //Identifying the column from where to read the "Data" value in the Row incase of "POST" or "PUT".
  val dataProperty = config("DataProperty", "").toString

  //Reading the authorization access token from config params
  val restApiAuthAccessToken = config("RestAPIAuthAccessToken").toString

  //Identifying the column from where to read the "CorrelationId" value in the Row.
  //If not specified default value is "CorrelationId".
  //This is currently not used, but can be enabled to pass the correlation Id in http request header if required.
  val correlationIdProperty = config("CorrelationIdProperty", "CorrelationId").toString


  //Returning the RDD of rows which should have the Rest Api response details along with existing columns
  //in the given table
  val restResponseRDD: RDD[Row] = {

    var responseRDD: RDD[Row] = null

    //Getting the input dataframe based on config params
    val dataFrame = getDataFrame

    val columns = dataFrame.columns

    val totalRows = dataFrame.count

    //Identifying the max parallel http requests to be made. Since there can
    //be multiple rows in input dataframe, ensuring that we are not chocking the
    //Rest Api service.
    val maxParallelRequests = config("restcall_maxparallelcalls", "20").toString.toInt

    var partitions = 1

    //Setting partitions based max parallel http requests that can be made
    if (totalRows > maxParallelRequests)
      partitions = maxParallelRequests

    //Repartitioning input dataframe and setting Rest Api response for each row
    responseRDD = dataFrame.rdd.repartition(partitions).map((r) =>
    {
      getRow(r,columns)
    })
    responseRDD

  }


  override def sqlContext: SQLContext = sparkSession.sqlContext

  /**
   * Setting the  schema of the final dataframe that the rest api connector would return
   * @return
   */
  override def schema: StructType = getOutputSchema

  /**
   * Returning the schema of the final dataframe that the rest api connector would return
   * @return
   */
  def getOutputSchema:StructType= {

    var _outputSchema = schemaProvided

    //Adding new columns based on Rest Api Response for each row
    _outputSchema = _outputSchema.add(new StructField("RestCall_HttpStatusCode", StringType))
    _outputSchema = _outputSchema.add(new StructField("RestCall_Response", StringType))
    _outputSchema = _outputSchema.add(new StructField("RestCall_StartTimeUTC", StringType))
    _outputSchema = _outputSchema.add(new StructField("RestCall_DurationMs", LongType))
    _outputSchema = _outputSchema.add(new StructField("RestCall_Retry", IntegerType))
    _outputSchema = _outputSchema.add(new StructField("RestCall_Host", StringType))

    _outputSchema

  }

  override def buildScan(): RDD[Row] = restResponseRDD

  /**
   * Return the Row appending the Rest Api response attributes
   * @param r
   * @param columns
   * @return
   */
  def getRow(r:Row,
             columns:Array[String])= {

    //Reading the Resource Url value for the current Row
    val resourceUriPropertyIndex = columns.indexOf(resourceUriProperty)
    val uri = r.getString(resourceUriPropertyIndex)

    //Making rest call for current Row and get response
    val restResponse = getRestResponse(uri, r, columns)

    val rowData = r.toSeq.toList

    val restResponseSeq=Seq(restResponse.httpStatusCode ,restResponse.responseBody ,restResponse.startTimeUTC ,restResponse.durationMs ,restResponse.retryCount ,restResponse.hostName)

    //Append Rest Api response details to current row
    Row.fromSeq(rowData.union(restResponseSeq))

  }


  /**
   * Get Rest Api response for given Url and details in given Row
   * @param uri
   * @param r
   * @param columns
   * @return
   */
  def getRestResponse(uri:String,
                      r:Row,
                      columns:Array[String]):HttpClientResponse={

    //Fetching correlation Id and data for current Row
    val correlationId = getCorrelationId(r,columns)
    val data=getData(r,columns)

    //Incase of "POST" or "PUT", throwing exception if "DataProperty" is not sent to connector
    ((restCallVerb == "POST" || restCallVerb == "PUT") && data == null) match {
      case true => {
        throw new Exception(s"RestCall_Verb value is $restCallVerb. Couldn't find DataProperty value set to a column to read data.")
      }
      case _ => {}
    }

    //Making rest call and returning the response
    val restResponse = restClient.call(restCallVerb, uri, data, restApiAuthAccessToken, correlationId,
      restCallRequestHeaders)

    restResponse

  }

  /**
   * Returns data that has to posted to Rest Api from given Row
   * @param r
   * @param columns
   * @return
   */
  def getData(r:Row,
              columns:Array[String])={

    //Reading "Data" from given row
    var data: String = null
    if (dataProperty != "") {
      val postDataPropertyIndex = columns.indexOf(dataProperty)
      data = r.getString(postDataPropertyIndex)
    }
    data

  }

  /**
   * Returns CorrelationId from given Row
   * @param r
   * @param columns
   * @return
   */
  def getCorrelationId(r:Row,columns:Array[String]):String={

    //Reading "CorrelationId" from given row
    val correlationIdPropertyIndex = columns.indexOf(correlationIdProperty)
    r.getString(correlationIdPropertyIndex)

  }

  /**
   * Return dataframe based on config parameters
   * @return
   */
  def getDataFrame:DataFrame={

    //Reading connector properties to create dataframe by
    //Location with Format or Table name
    val sourceLocation = config("Location", "").toString
    val sourceFormat = config("Format", "parquet").toString
    val sourceTable = config("Table", "").toString

    //Raising exception if both table and location are not sent to connector
    if (sourceTable == "" && sourceLocation == "")
      throw new Exception("Please specify 'Location' or 'Table' in options to read the data and invoking rest api.")

    var dataFrame: DataFrame = null

    //Constructing dataframe either by location or table
    if (sourceFormat != "" && sourceLocation != "" && sourceFormat != null && sourceLocation !=null) {
      dataFrame = sparkSession.read.format(sourceFormat).load(sourceLocation)
    }
    if (sourceTable != "" && sourceTable !=null) {
      dataFrame = sparkSession.sql(s"select * from $sourceTable")
    }
    dataFrame

  }

}
