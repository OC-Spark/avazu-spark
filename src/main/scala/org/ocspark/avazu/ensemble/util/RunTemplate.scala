package org.ocspark.avazu.ensemble.util

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.FMWithLBFGS
import org.apache.spark.mllib.util.MLUtils
import org.ocspark.avazu.Common
import org.ocspark.avazu.ensemble.Cvt
import scopt.OptionParser
import org.ocspark.avazu.ConvertLibSVM

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
      run(params.size, "app", params.filterString, params.iter, null, sc)
    }.getOrElse {
      sys.exit(1)
    }
  }
  
  def run(size : String, filterString : String, iter : String, model : String, cvt : Cvt, sc : SparkContext){
    Common.createDir(model)
    subset(filterString, size, model, sc)
    genData(size, model, sc)
    runCvt(size, model, cvt, sc)
    
    if (nonZeroLines(size, model, sc)){
	    trainFM(size, model, sc)
	    trainFM(size, model, sc)
	    println("trainFM done")
	    mkPrd(size, model, sc)
	    calcLoss(size, model, sc)
    }
  }
  
  def nonZeroLines(size : String, model : String, sc : SparkContext) : Boolean = {
    val trainingSp = s"/avazu/ensemble/$model/tr.r$size.sp"
    val trCount = sc.textFile(trainingSp, 4).count
    val validSp = s"/avazu/ensemble/$model/va.r$size.sp"
    val vaCount = sc.textFile(validSp, 4).count
    
    trCount > 0 | vaCount > 0
  }
  
  def subset(filterString : String, size : String, model : String, sc : SparkContext){
    Subset.run(filterString, s"/avazu/tr.r$size.csv", s"/avazu/va.r$size.csv", s"/avazu/ensemble/$model/tr.r$size.sub.csv", s"/avazu/ensemble/$model/va.r$size.sub.csv", sc)
  }
  
  def genData(size : String, model : String, sc : SparkContext){
    GenData.run(s"/avazu/ensemble/$model/tr.r$size.sub.csv", s"/avazu/ensemble/$model/va.r$size.sub.csv", s"/avazu/ensemble/$model/tr.r$size.new.csv", s"/avazu/ensemble/$model/va.r$size.new.csv", sc)
  }
  
  def runCvt(size : String, model : String, cvt : Cvt, sc : SparkContext){
    cvt.convert(s"/avazu/ensemble/$model/tr.r$size.new.csv", s"/avazu/ensemble/$model/tr.r$size.sp", true, sc)
    cvt.convert(s"/avazu/ensemble/$model/va.r$size.new.csv", s"/avazu/ensemble/$model/va.r$size.sp", true, sc)
  }
  
  def trainFM(size : String, model : String, sc : SparkContext){
    val trainingSp = s"/avazu/ensemble/$model/tr.r$size.sp"
    val lineCount = ConvertLibSVM.addOne(trainingSp, trainingSp + ".svm", sc)		// append :1 behind each index
    val training = MLUtils.loadLibSVMFile(sc, "hdfs://localhost/" + trainingSp + ".svm").cache()
    val fm = FMWithLBFGS.train(training, task = 1, numIterations = 20, numCorrections = 5, dim = (true, true, 4), regParam = (0, 0, 0), initStd = 0.1)

    val validSb = s"/avazu/ensemble/$model/va.r$size.sp"
    ConvertLibSVM.addOne(validSb, validSb + ".svm", sc)		// append :1 behind each index
    val valid = MLUtils.loadLibSVMFile(sc, "hdfs://localhost/" + validSb + ".svm").cache()
    val prediction = fm.predict(valid.map(_.features))
    val logLoss = Common.calcLogLoss(valid, prediction)
    println("app validation logloss = " + logLoss)
    
    val sb = new StringBuilder()
    prediction.collect.foreach {
      p => sb.append(p + "\n")
    }
    
    Common.writeOut(sb.toString, s"/avazu/ensemble/$model/va.r$size.out")

  }
  
  def mkPrd(size : String, model : String, sc : SparkContext){
    MkPrd.run(s"/avazu/ensemble/$model/va.r$size.sub.csv", s"/avazu/ensemble/$model/va.r$size.out", s"/avazu/pool/$model.r$size.prd", sc)
  }
  
  def calcLoss(size : String, model: String, sc : SparkContext){
    CalcLoss.run(s"/avazu/pool/$model.r$size.prd", s"/avazu/ensemble/$model/va.r$size.sub.csv", sc)
  }

}