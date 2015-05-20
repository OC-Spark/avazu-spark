package org.ocspark.avazu.base

import org.ocspark.avazu.base.util.GenData
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.ocspark.avazu.base.converter.Converter2
import org.ocspark.avazu.ConvertLibSVM
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.FMWithLBFGS
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.FMModel
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.ocspark.avazu.Common

object RunBase {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sparkConf = new SparkConf().setAppName("GenData")
      .setMaster("local[4]") // comment out when submitting to spark cluster

    val sc = new SparkContext(sparkConf)
    
    run(sc)
  }

  def run(sc: SparkContext) {
    val trSrcPath = "/avazu/tr.rx.csv"
    val vaSrcPath = "/avazu/va.rx.csv"
    val trAppDstPath = "/avazu/base/tr.rx.app.new.csv"
    val vaAppDstPath = "/avazu/base/va.rx.app.new.csv"
    val trSiteDstPath = "/avazu/base/tr.rx.site.new.csv"
    val vaSiteDstPath = "/avazu/base/va.rx.site.new.csv"

    GenData.run(trSrcPath, vaSrcPath, trAppDstPath, vaAppDstPath, trSiteDstPath, vaSiteDstPath, sc)

    val trAppSpPath = "/avazu/base/tr.rx.app.sp"
    val vaAppSpPath = "/avazu/base/va.rx.app.sp"
    Converter2.convert(trAppDstPath, trAppSpPath, true, sc)
    Converter2.convert(vaAppDstPath, vaAppSpPath, false, sc)

    val trSiteSpPath = "/avazu/base/tr.rx.site.sp"
    val vaSiteSpPath = "/avazu/base/va.rx.site.sp"
    Converter2.convert(trSiteDstPath, trSiteSpPath, true, sc)
    Converter2.convert(vaSiteDstPath, vaSiteSpPath, false, sc)

    val trAppLibSVMFile = trAppSpPath + ".svm"
    ConvertLibSVM.addOne(trAppSpPath, trAppLibSVMFile, sc)
    val training = MLUtils.loadLibSVMFile(sc, "hdfs://localhost/" + trAppLibSVMFile).cache()
    val trAppFm = FMWithLBFGS.train(training, task = 1, numIterations = 20, numCorrections = 5, dim = (true, true, 4), regParam = (0, 0, 0), initStd = 0.1)
    // calc loss for training data

    val vaAppLibSVMFile = vaAppSpPath + ".svm"
    ConvertLibSVM.addOne(vaAppSpPath, vaAppLibSVMFile, sc)
    // do prediction and calc loss
    val appValidation = MLUtils.loadLibSVMFile(sc, vaAppLibSVMFile).cache()
    //    println("app validation count = " + appValidation.count)
    //    validation.cache
    val appPrediction = trAppFm.predict(appValidation.map(_.features))
    //    println("app prediction count = " + appPrediction.count)
    val appIdRDD = sc.textFile(vaAppSpPath, 4).map {
      line =>
        val id = line.split(" ", 2)
        id(0)
    }
    val appIdPrediction = Common.genPerEventPrediction(appIdRDD, appPrediction)

    val logLoss = Common.calcLogLoss(appValidation, appPrediction)
    println("app validation logloss = " + logLoss)

    ConvertLibSVM.addOne(trSiteSpPath, trSiteSpPath + ".svm", sc)
    val trSite = MLUtils.loadLibSVMFile(sc, "hdfs://localhost/" + trSiteSpPath + ".svm").cache()
    val trSiteFm = FMWithLBFGS.train(trSite, task = 1, numIterations = 20, numCorrections = 5, dim = (true, true, 4), regParam = (0, 0, 0), initStd = 0.1)

    val vaSiteLibSVMFile = vaSiteSpPath + ".svm"
    ConvertLibSVM.addOne(vaSiteSpPath, vaSiteLibSVMFile, sc)

    val siteValidation = MLUtils.loadLibSVMFile(sc, vaSiteLibSVMFile).cache()
    //    println("site validation count = " + siteValidation.count)
    val sitePrediction = trSiteFm.predict(siteValidation.map(_.features))
    //    println("site prediction count = " + sitePrediction.count)

    val siteLogLoss = Common.calcLogLoss(siteValidation, sitePrediction)
    println("site validation logloss = " + logLoss)

    val siteIdRDD = sc.textFile(vaSiteSpPath, 4).map {
      line =>
        val id = line.split(" ", 2)
        id(0)
    }
    // merge results 
    val siteIdPrediction = Common.genPerEventPrediction(siteIdRDD, sitePrediction)
    val mergedPrediction = siteIdPrediction.union(appIdPrediction)
      .groupByKey()
      .map {
        predictions =>
          val merged = Common.mergePredictions(predictions._2)
          merged
      }
    mergedPrediction.cache
    println("merged prediction count = " + mergedPrediction.count)

    val unionedValidation = siteValidation.union(appValidation)
    val validationCount = unionedValidation.count
    val mergedPredictionCount = mergedPrediction.count
    if (validationCount == mergedPredictionCount) {
      val mergedLogLoss = Common.calcLogLoss(unionedValidation, mergedPrediction)
      println("site prediction count = " + sitePrediction.count)
    } else {
      println("Redundant ad id's exist!")
    }

  }

}