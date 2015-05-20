package org.ocspark.avazu.ensemble.util

import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext

import scopt.OptionParser

object RunTemplate {

  def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("Group6")
      .setMaster("local[4]") // comment out when submitting to spark cluster

    val sc = new SparkContext(sparkConf)

    val defaultParams = RunTemplateParams()

    val parser = new OptionParser[RunTemplateParams]("SparseNaiveBayes") {
      head("SparseNaiveBayes: an example naive Bayes app for LIBSVM data.")
      opt[Double]("reg")
        .text("the index of the first column in group features ")
        .action((x, c) => c.copy(reg = x))
      opt[Double]("eta")
        .text("specified the maximum number of count features. Any feature with counts less than the value would be replaced with its count.")
        .action((x, c) => c.copy(eta = x))
      opt[String]("mark")
        .text(s"the upper limit of the size of each group, default: 100")
        .action((x, c) => c.copy(mark = x))
      arg[String]("size")
        .text("size")
        .required()
        .action((x, c) => c.copy(size = x))
      arg[String]("filter_string")
        .text("filter string")
        .required()
        .action((x, c) => c.copy(filterString = x))
      arg[String]("iter")
        .text("iter")
        .required()
        .action((x, c) => c.copy(iter = x))

    }

    parser.parse(args, defaultParams).map { params =>
      run(params.size, params.filterString, params.iter, sc)
    }.getOrElse {
      sys.exit(1)
    }
  }
  
  def run(size : String, filterString : String, iter : String, sc : SparkContext){
    subset()
    genData()
    cvt("app")
    cvt("site")
    
    runFm()
    mkPrd()
    calcLoss()
  }
  
  def subset(){
    Subset.run()
  }
  
  def genData(){
    
  }
  
  def cvt(category : String){
    
  }
  
  def runFm(){
    
  }
  
  def mkPrd(){
    
  }
  
  def calcLoss(){
    
  }

}