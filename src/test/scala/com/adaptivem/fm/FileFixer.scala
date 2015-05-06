package com.adaptivem.fm

import scala.io.Source
import java.io.File
import java.io.FileWriter
import util.control.Breaks._

object FileFixer {

  def main(args: Array[String]): Unit = {
    val toDir = new File("/home/bruce/Documents/machine learning/data/fixed/")
    val dir = new File("/home/bruce/Documents/machine learning/data/")
    for (f <- dir.listFiles()){
      breakable {
        if (f.isDirectory()){
        break
      }
      val fw = new FileWriter("/home/bruce/Documents/machine learning/data/fixed/" + f.getName, true)
      for(line <- Source.fromFile(f).getLines()){
        fw.write(line + "\n")
      }
      fw.close()
      }
    }
  }
}