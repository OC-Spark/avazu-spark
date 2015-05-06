package org.ocspark.avazu.base.util

object Parallelizer {

  def main(args: Array[String]): Unit = {
    val cvtPath = args(0)
    val trSrcPath = args(1)
    val vaSrcPath = args(2)
    val trDstPath = args(3)
    val vaDstPath = args(4)
 
    val nr_thread = 12
    
    /*split(trSrcPath, nr_thread, true)
    split(vaSrcPath, nr_thread, true)

    parallel_convert(cvtPath, trSrcPath, vaSrcPath,trDstPath, vaDstPath, nr_thread)

    delete(trSrcPath, nr_thread)
    delete(vaSrcPath, nr_thread)

    cat(trDstPath, nr_thread)
    cat(vaDstPath, nr_thread)

    delete(trDstPath, nr_thread)
    delete(vaDstPath, nr_thread)*/
    
  }
  
}