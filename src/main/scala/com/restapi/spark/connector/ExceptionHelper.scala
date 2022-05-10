package com.restapi.spark.connector

/**
 * Exception helper object
 */

object ExceptionHelper {

  /**
   * Fetching exception details including inner exceptions in recursive
   * @param exp
   * @return
   */
  def getExceptionDetailMessage(exp:Throwable):String={

    var message=exp.toString()+". "+ exp.getMessage
    val innerException=exp.getCause
    if(innerException!=null){
      message=message+"\n"+getExceptionDetailMessage(innerException)
    }
    message

  }
}