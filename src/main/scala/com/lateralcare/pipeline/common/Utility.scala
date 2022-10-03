package com.lateralcare.pipeline.common

import java.nio.file.Paths

case object Utility{

  def getFileExtension(filePath: String): String = {
    // -- Split out the file extension from the file path
    val fileName = Paths.get(filePath).getFileName
    fileName.toString.split("\\.").last
  }

  def getClientIDFromFilePath(filePath: String): String =
  {
    // -- Split out the client map id from the file path
    val pathSplit = filePath.split("/")
    pathSplit(pathSplit.length - 2)
  }
}
