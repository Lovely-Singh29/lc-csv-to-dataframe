import com.lateralcare.pipeline.stage.unzip.UnzipS3
import com.lateralcare.pipeline.stage.unzip.Unzip
import org.scalatest.funsuite.AnyFunSuite


class UnzipTest extends AnyFunSuite{

  test("UnzipSuccess"){
    val unzip_Files = Array("./src/main/resources/patient-account-data.gz","./src/main/resources/save")
    //Unzip.main(unzip_Files)
    UnzipS3.unzipS3("TestFolder/","OutPutFolder/")
  }

  test("UnzipFailure") {
    val unzip_Files = Array("" ,"./src/main/resources/save")
    Unzip.main(unzip_Files) == true
  }

  test("UnzipNullFailure") {
    val filepath = null
    val de_filepath = null
    assert(Unzip.unZipIt(filepath, de_filepath) == false)
  }
}
