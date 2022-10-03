package com.lateralcare.pipeline.stageexecuter

class StageArgument {

  private[this] var _sourcePath: String = ""

  def getSourcePath() = { _sourcePath }

  def setSourcePath(value: String): Unit = {
    _sourcePath = value
  }

  private[this] var _destinationPath: String = ""

  def getDestinationPath() : String = { _destinationPath }

  def setDestinationPath(value: String): Unit = {
    _destinationPath = value
  }

  private[this] var _lookupQuery: String = ""

  def getLookupQuery() : String = { _lookupQuery }

  def setLookupQuery(value: String): Unit = {
    _lookupQuery = value
  }

  private[this] var _fileFilter: String = ""

  def getFileFilter() : String = { _fileFilter }

  def setFileFilter(value: String): Unit = {
    _fileFilter = value
  }

}