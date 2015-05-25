package org.ocspark.avazu.base

import org.ocspark.avazu.base.util.GenBaseData
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
import scala.collection.mutable.ArrayBuffer

object RunBase {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sparkConf = new SparkConf().setAppName("GenData")
      .setMaster("local[4]") // comment out when submitting to spark cluster

    val sc = new SparkContext(sparkConf)
    
    run("x", sc)
  }

  def run(size : String, sc: SparkContext) {
    val trSrcPath = s"/avazu/tr.r$size.csv"
    val vaSrcPath = s"/avazu/va.r$size.csv"
    val trAppDstPath = s"/avazu/base/tr.r$size.app.new.csv"
    val vaAppDstPath = s"/avazu/base/va.r$size.app.new.csv"
    val trSiteDstPath = s"/avazu/base/tr.r$size.site.new.csv"
    val vaSiteDstPath = s"/avazu/base/va.r$size.site.new.csv"

    GenBaseData.run(trSrcPath, vaSrcPath, trAppDstPath, vaAppDstPath, trSiteDstPath, vaSiteDstPath, sc)

    val trAppSpPath = "/avazu/base/tr.rx.app.sp"
    val vaAppSpPath = "/avazu/base/va.rx.app.sp"
    Converter2.convert(trAppDstPath, trAppSpPath, true, sc)
    Converter2.convert(vaAppDstPath, vaAppSpPath, false, sc)

    val trSiteSpPath = "/avazu/base/tr.rx.site.sp"
    val vaSiteSpPath = "/avazu/base/va.rx.site.sp"
    Converter2.convert(trSiteDstPath, trSiteSpPath, true, sc)
    Converter2.convert(vaSiteDstPath, vaSiteSpPath, false, sc)
    
    val appPred_Label = predict(trAppSpPath, vaAppSpPath, sc)
    val appIdPrediction = appPred_Label._1
    val appValidation = appPred_Label._2

    val sitePred_label = predict(trSiteSpPath, vaSiteSpPath, sc)
    val siteIdPrediction = sitePred_label._1
    
    // merge results 
    val mergedIdPredictions = siteIdPrediction.union(appIdPrediction)
      .groupByKey()
      .map {
        id_prediction =>
          val id = id_prediction._1
          val merged = Common.mergePredictions(id_prediction._2)
//          println("merged ="+ merged)
          (id, merged)
      }
    mergedIdPredictions.cache
    val idPredictionArray = ArrayBuffer[String]()
    mergedIdPredictions.collect.foreach {
      idPrediction =>
        idPredictionArray.append(idPrediction._1 + "," + idPrediction._2)
    }
    Common.writeOut(idPredictionArray.toArray.mkString("\n"), s"/avazu/pool/base.r$size.prd")
    
    val mergedPredictions = mergedIdPredictions.map {
      id_prediction =>
        val prediction = id_prediction._2
        prediction
    }

    val siteValidation = sitePred_label._2
    val validationCount = siteValidation.count
    val mergedPredictionCount = mergedPredictions.count
    println("merged prediction count = " + mergedPredictionCount)
    
    if (validationCount == mergedPredictionCount) {
      val mergedLogLoss = Common.calcLogLoss(siteValidation, mergedPredictions)
    } else {
      println("Redundant ad id's exist!")		// bad test data set was used in avazu 
    }

  }
  
  def predict(trSpPath : String, vaSpPath : String, sc : SparkContext) : (RDD[(String, Double)], RDD[LabeledPoint]) = {
    ConvertLibSVM.addOne(trSpPath, trSpPath + ".svm", sc)		// append :1 behind each index
    val trSite = MLUtils.loadLibSVMFile(sc, "hdfs://localhost/" + trSpPath + ".svm").cache()
    val trSiteFm = FMWithLBFGS.train(trSite, task = 1, numIterations = 20, numCorrections = 5, dim = (true, true, 4), regParam = (0, 0, 0), initStd = 0.1)

    val vaSiteLibSVMFile = vaSpPath + ".svm"
    ConvertLibSVM.addOne(vaSpPath, vaSiteLibSVMFile, sc)

    val validation = MLUtils.loadLibSVMFile(sc, vaSiteLibSVMFile).cache()
    //    println("site validation count = " + siteValidation.count)
    val sitePrediction = trSiteFm.predict(validation.map(_.features))
    //    println("site prediction count = " + sitePrediction.count)

    val logLoss = Common.calcLogLoss(validation, sitePrediction)
    println("site validation logloss = " + logLoss)

    val idRDD = sc.textFile(vaSpPath, 4).map {
      line =>
        val id = line.split(" ", 2)
        id(0)
    }
    // merge results 
    val idPrediction = Common.genPerEventPrediction(idRDD, sitePrediction)
    
    (idPrediction, validation)
  }

}