package com.lateralcare.pipeline.stage.unzip

import com.lateralcare.pipeline.common.{StageFailedArgument, StageFailedException}
import com.lateralcare.pipeline.stage.entrypoint.`trait`.PipelineStage
import com.lateralcare.pipeline.stage.entrypoint.model.StageArgument
import com.lateralcare.pipeline.stage.unzip.UnzipS3.{destinationPath, getStageName, sourcePath}
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream

import java.io.{File, FileInputStream, FileOutputStream, IOException}


object Unzip extends PipelineStage {





  def unZipIt(zipFile: String, outputFolder: String): Boolean = {



    val buffer = new Array[Byte](1024)

    var boolean: Boolean = false

    try {

      //output directory
      val folder = new File(outputFolder);
      System.out.println(outputFolder);
      if (!folder.exists()) {
        folder.mkdir();
      }

      //zip file content
      val zis: GzipCompressorInputStream = new GzipCompressorInputStream(new FileInputStream(zipFile));

      //get the zipped file list entry
      var ze = zis.read(buffer);

      while (ze >= 0) {

        val fileName = zis.getMetaData().getFilename()

        val newFile = new File(outputFolder + File.separator + fileName);

        System.out.println("file unzip : " + newFile.getAbsoluteFile());

        //create folders
        new File(newFile.getParent()).mkdirs();

        val fos = new FileOutputStream(newFile);

        var len: Int = zis.read(buffer);

        while (len > 0) {

          fos.write(buffer, 0, len)
          len = zis.read(buffer)
        }

        fos.close()
        ze = zis.read(buffer);
      }

      zis.close()
      boolean = true
    }catch {
      case e: Exception => println("exception caught: " + e.getMessage)
        boolean = false
    }
   boolean
  }

  override var sourcePath = ""
  override var destinationPath = ""

  override def getStageName() = {
    "Unzip"
  }

  override def getStageId() = {
    "123"
  }

  override def Run(stageArgument: StageArgument) = {

    sourcePath = stageArgument.getSourcePath()
    destinationPath = stageArgument.getDestinationPath()


    try{
      Unzip.unZipIt(sourcePath,destinationPath)
    }
    catch {

      case ex: Exception => {
        val stageFailedArgument = new StageFailedArgument()
        stageFailedArgument.setStageName(getStageName())
        stageFailedArgument.setErrorMessage(ex.getMessage)
        stageFailedArgument.setSourcePath(sourcePath)
        stageFailedArgument.setDestinationPath(destinationPath)
        stageFailedArgument.setStackTrace(ex.getStackTrace)

        throw new StageFailedException(stageFailedArgument)
      }
    }
    true
  }
}
