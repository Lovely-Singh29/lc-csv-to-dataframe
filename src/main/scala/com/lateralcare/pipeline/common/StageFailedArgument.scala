package com.lateralcare.pipeline.common

class StageFailedArgument {

  private[this] var _stageName: String = null

  def getStageName(): String = { _stageName }

  def setStageName(value: String): Unit = {
    _stageName = value
  }

  private[this] var _sourcePath: String = null

  def getSourcePath() : String = { _sourcePath }

  def setSourcePath(value: String): Unit = {
    _sourcePath = value
  }

  private[this] var _destinationPath: String = null

  def getDestinationPath(): String = { _destinationPath }

  def setDestinationPath (value: String): Unit = {
    _destinationPath = value
  }

  private[this] var _errorMessage: String = null

  def getErrorMessage(): String = { _errorMessage }

  def setErrorMessage (value: String): Unit = {
    _errorMessage = value
  }

  private[this] var _stackTrace: Array[StackTraceElement] = null

  def getStackTrace(): Array[StackTraceElement] = _stackTrace

  def setStackTrace (value: Array[StackTraceElement]): Unit = {
    _stackTrace = value
  }


}
