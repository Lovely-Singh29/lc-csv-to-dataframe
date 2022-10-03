package com.lateralcare.pipeline.stage.entrypoint.`trait`

import com.lateralcare.pipeline.common.StageFailedException
import com.lateralcare.pipeline.stage.entrypoint.model.StageArgument

trait PipelineStage extends App {

  var sourcePath: String
  var destinationPath: String

  def getStageName(): String

  def getStageId(): String

  @throws(classOf[StageFailedException])
  def Run(stageArgument: StageArgument): Boolean

}
