package com.lateralcare.pipeline.stageexecuter

import org.reflections.Reflections

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable.ListBuffer

object PipelineStageFactory {


  var subClassesMap:collection.mutable.Map[String,String] = collection.mutable.Map() // Holds [ Classname, com.xxx.ClassName ]
  var subClasses: List[String] = List() // Holds [ com.xxx.ClassName1, com.xxx.ClassName2 ]

  PipelineStageFactory.loadStages()

  private def loadStages() ={

    val reflections = new Reflections("com.lateralcare")
    val subTypes = reflections.getSubTypesOf(classOf[PipelineStage])
    //println("subTypes --" + subTypes)
    var subClassesBuf = new ListBuffer[String]()

    subTypes.map(c => {
      //println("class names --" + c.getSimpleName)
      val stageClass = c.getName.replace("$", "")
      val simpleStageClass = c.getSimpleName.replace("$", "")

      //println("stageClass--" + stageClass)
      subClassesBuf += stageClass // Appending all the sub classes into List
      subClassesMap += (simpleStageClass -> stageClass) // Storing Class Name and Class Name with package as key value
    })

    subClasses = subClassesBuf.toList
  }
}