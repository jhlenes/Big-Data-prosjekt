import java.nio.file.Files

object ResultManager {

  def moveResult(resultDirectory: String): Unit = {
    import java.nio.file.Paths
    import java.nio.file.StandardCopyOption
    Files.move(Paths.get(resultDirectory + "/part-00000"), Paths.get(resultDirectory + ".txt"), StandardCopyOption.REPLACE_EXISTING)
    deletePreviousResult(resultDirectory)
  }

  def deletePreviousResult(resultDirectory: String): Unit = {
    if (new java.io.File(resultDirectory).exists) {
      import org.apache.commons.io.FileUtils
      FileUtils.deleteDirectory(new java.io.File(resultDirectory))
    }
  }

}
