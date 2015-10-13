package interretis.utils

import java.nio.file.{ Files, Paths }
import language.postfixOps

object FileSystemUtils {

  def createTempDirectory(root: String, prefix: String): String = {
    val temp = Files createTempDirectory (Paths get root, prefix)
    temp toString
  }

  def buildPath(first: String, second: String): String = {
    Paths get (first, second) toString
  }
}
