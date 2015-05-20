package org.ocsparks.fm

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration

object FileCopier {
  val conf = new Configuration()
  val hdfsCoreSitePath = new Path("core-site.xml")
  conf.addResource(hdfsCoreSitePath)
  val fs = FileSystem.get(conf);

  def main(args: Array[String]): Unit = {
    val files = new java.io.File("/home/bruce/Documents/machine learning/data").listFiles
    files.foreach { 
      file => println(file.getName) 
      val local = new Path(file.getAbsolutePath);
      val hdfs = new Path("/user/adaptive/fm/data/" + local.getName());
      fs.copyFromLocalFile(false, false, local, hdfs)
    }
  }
}