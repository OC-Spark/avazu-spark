package org.ocspark.avazu.bag.run

import org.ocspark.avazu.bag.util.CountFeat
import org.ocspark.avazu.bag.converter.Converter6
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.ocspark.avazu.bag.converter.Group6
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.ocspark.avazu.bag.converter.Group6Params
import org.ocspark.avazu.bag.util.JoinData
import org.apache.spark.mllib.regression.FMWithLBFGS
import org.ocspark.avazu.ConvertLibSVM
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.ocspark.avazu.Common
import org.ocspark.avazu.bag.util.CatId
import org.ocspark.avazu.bag.util.CatSubmit

object RunBag {

  def main(args: Array[String]): Unit = {
    val size = args(0)
    val min_occur = args(1).toInt

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sparkConf = new SparkConf().setAppName("Convert6")
      .setMaster("local[1]") // comment out when submitting to spark cluster
    val sc = new SparkContext(sparkConf)
    
    run(size, min_occur, sc)
  }
  
  def run(size : String, min_occur : Int, sc : SparkContext){
    val tr_path = s"/avazu/tr.r$size.csv"
    val va_path = s"/avazu/va.r$size.csv"

    val group_counts = CountFeat.countFeatures(tr_path, va_path, min_occur, sc)
    
    runSteps(size, "app", group_counts, sc)
    runSteps(size, "site", group_counts, sc)
    
    catSubmit(size, sc)
  }
  
  def runSteps(size : String, category : String, group_counts : Map[String, Map[String, Int]], sc : SparkContext){
    // do 6.py
    // convert6
    convert6(size, category, sc)
    
    // group6
    group6(group_counts, size, category, sc)
    
     // join_data
    joinData(size, category, sc)
    
    // convert to libSVM
    convertToLibSVM(size, category, sc)
    
    // libFM
    trainFM(size, category, sc)
    
    // cat_id_click
    catIdClick(size, category, sc)
        
  }

  def convert6(size: String, category: String, sc: SparkContext) {
    val tr_src_path = s"/avazu/base/tr.r$size.$category.new.csv"
    val tr_dst_path = s"/avazu/bag/tr.r$size.$category.sp"
    Converter6.convert(tr_src_path, tr_dst_path, true, sc)

    val va_src_path = s"/avazu/base/va.r$size.$category.new.csv"
    val va_dst_path = s"/avazu/bag/va.r$size.$category.sp"
    Converter6.convert(va_src_path, va_dst_path, false, sc)

  }
  
  def group6(groupCounts : Map[String, Map[String, Int]], size : String, category : String, sc : SparkContext) {
    val train = s"/avazu/base/tr.r$size.$category.new.csv" 
    val valid = s"/avazu/base/va.r$size.$category.new.csv" 
    val g_field = "device_id"
    val a_field = "pub_id,pub_domain"
//    println("call group6 train = " + train + ", valid = " + valid)
    val params = Group6Params(train = train, valid = valid, partition = category, a_field = a_field, g_field = g_field)

    Group6.run(params, groupCounts, sc)
  }
  
  def joinData(size : String, category : String, sc : SparkContext){
    JoinData.run(s"/avazu/bag/tr.r$size.$category.sp", s"/avazu/bag/tr.r$size.$category.new.csv.group", sc)
    JoinData.run(s"/avazu/bag/va.r$size.$category.sp", s"/avazu/bag/va.r$size.$category.new.csv.group", sc)
    
  }
  
  def convertToLibSVM(size : String, category : String, sc : SparkContext){
    ConvertLibSVM.stripField(s"/avazu/bag/tr.r$size.$category.sp.join", s"/avazu/bag/tr.r$size.$category.sp.join.svm", sc)
    ConvertLibSVM.stripField(s"/avazu/bag/va.r$size.$category.sp.join", s"/avazu/bag/va.r$size.$category.sp.join.svm", sc)
  }
  
  def trainFM(size : String, category : String, sc : SparkContext){
//    cmd = './mark18 -r 0.05 -s 1 -t 6 va.r{size}.{category}.sp.join tr.r{size}.{category}.sp.join'.format(size=SIZE, category=CATEGORY) 
    val trainingSvm = s"/avazu/bag/tr.r$size.$category.sp.join.svm"
    val training = MLUtils.loadLibSVMFile(sc, "hdfs://localhost/" + trainingSvm).cache()
    val fm = FMWithLBFGS.train(training, task = 1, numIterations = 20, numCorrections = 5, dim = (true, true, 4), regParam = (0, 0, 0), initStd = 0.1)

    val validSvm = s"/avazu/bag/va.r$size.$category.sp.join.svm"
    val valid = MLUtils.loadLibSVMFile(sc, "hdfs://localhost/" + validSvm).cache()
    val prediction = fm.predict(valid.map(_.features))
    val logLoss = Common.calcLogLoss(valid, prediction)
    println("app validation logloss = " + logLoss)
    
    val sb = new StringBuilder()
    prediction.collect.foreach {
      p => sb.append(p + "\n")
    }
    
    Common.writeOut(sb.toString, s"/avazu/bag/va.r$size.$category.sp.join.out")

  }
  
  def catIdClick(size : String, category : String, sc : SparkContext){
    val sp = s"/avazu/bag/va.r$size.$category.sp"
    val out = s"/avazu/bag/va.r$size.$category.sp.join.out"
    val submit = s"/avazu/bag/va.r$size.$category.submit"

    CatId.run(sp, out, submit, sc)
    
  }
  
  def catSubmit(size : String, sc : SparkContext){
    val fApp = s"/avazu/bag/va.r$size.app.submit"
    val fSite = s"/avazu/bag/va.r$size.site.submit"
    val fOut = s"/avazu/pool/bag.r$size.prd"
    
    CatSubmit.run(fApp, fSite, fOut, sc)
    
  }

}
