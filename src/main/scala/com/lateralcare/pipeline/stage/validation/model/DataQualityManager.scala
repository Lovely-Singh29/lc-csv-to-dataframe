package com.lateralcare.pipeline.stage.validation.model

import com.lateralcare.pipeline.stage.validation.factory.FilePropertyValidatorFactory

class DataQualityManager {

  def validate(filePath: String ): Boolean = {

    // -- 1. Validate the file property of the given file type

    // -- Get FilePropertyValidator object using factory method
    val filePropertyValidator = FilePropertyValidatorFactory.apply(filePath)
    if( !filePropertyValidator.CheckFileFormat(filePath)) {
      // -- File property is not valid, so return false and lets not proceed
      false
    }
    else {
      // -- ToDO: Call other validators
      true
    }
  }
}
