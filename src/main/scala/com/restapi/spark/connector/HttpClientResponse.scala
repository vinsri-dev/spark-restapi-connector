package com.restapi.spark.connector


case class RestResponse(RequestUrl :String, httpStausCode:String,responseBody:String,startTimeUTC:String,durationMs:Long,retryCount:Int,hostName:String)