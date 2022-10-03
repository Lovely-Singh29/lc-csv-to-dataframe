package com.lateralcare.pipeline.stage.entrypoint.model

import org.json4s._
import org.json4s.jackson.JsonMethods._

/*
Pojo Class for serializing json string of Stage object
*/
case class Stage(stagename: String, stageid: String, stagedescription: String,
                 stageorder: Int, invoke: Boolean, intermStorageType: String,
                 sourcepath: String, destinationpath: String, lookupfilepath: String,
                 lookupspec: LookupSpec)

/*
Pojo Class for serializing json string of LookupSpec object
*/
case class LookupSpec(tool: String,
                      lookupfile: LookupFile,
                      sourcefile: sourcefile,
                      destinationfile: destinationfile,
                      query: Query)

/*
Pojo Class for serializing json string of LookupFile object
*/
case class LookupFile(filename: String, fileformat: String)

/*
Pojo Class for serializing json string of sourcefile object
*/
case class sourcefile(filename: String, fileformat: String)

/*
Pojo Class for serializing json string of destinationfile object
*/
case class destinationfile(filename: String, fileformat: String)

/*
Pojo Class for serializing json string of Query object
*/
case class Query(select: String, join: Join)

/*
Pojo Class for serializing json string of Join object
*/
case class Join(jointype: String, joincondition: Joincondition)

/*
Pojo Class for serializing json string of Joincondition object
*/
case class Joincondition(righttablecolumn: String, lefttablecolumn: String)

/*
Pojo class that holds the stage configuration and same will be passed to all Stages
 */
class StageArgument(stageSpecJson: String) {

  implicit val formats: Formats = DefaultFormats // Brings in default date formats etc.

  // -- Parse the incoming stage specific json to extract the stage object
  var stageSpec = parse(stageSpecJson)

  var stageSpecObject = stageSpec.extract[Stage]

  private[this] var _sourcePath: String = ""

  def getSourcePath() = { stageSpecObject.sourcepath }

  def setSourcePath(value: String): Unit = {
    _sourcePath = value
  }

  private[this] var _destinationPath: String = ""

  def getDestinationPath() : String = { stageSpecObject.destinationpath }

  def setDestinationPath(value: String): Unit = {
    _destinationPath = value
  }

  private[this] var _lookupFilePath: String = ""

  def getLookupFilePath(): String = {
    stageSpecObject.lookupfilepath
  }

  def setLookupFilePath(value: String): Unit = {
    _lookupFilePath = value
  }

  private[this] var _fileFilter: String = ""

  def getFileFilter() : String = { _fileFilter }

  def setFileFilter(value: String): Unit = {
    _fileFilter = value
  }

  private[this] var lookupspec: String = ""

  def getlookupspec(): LookupSpec = {
    stageSpecObject.lookupspec
  }

  def setlookupspec(value: String): Unit = {
    lookupspec = value
  }

}
