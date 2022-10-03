package com.lateralcare.pipeline.stageexecuter

import com.lateralcare.pipeline.common.StageFailedException

trait PipelineStage extends App {

  var sourcePath :String
  var destinationPath: String
  def getStageName(): String
  def getStageId(): String

  @throws(classOf[StageFailedException])
  def Run(stageArgument: StageArgument) : Boolean

}