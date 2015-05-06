package org.ocspark.avazu.base

import org.ocspark.avazu.base.util.GenData
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.ocspark.avazu.base.converter.Converter2

object Run {

  def main(args: Array[String]): Unit = {
    val trSrcPath = "avazu/tr.rx.csv"
    val vaSrcPath = "avazu/va.rx.csv"
    val trAppDstPath = "avazu/base/tr.rx.app.new.csv"
    val vaAppDstPath = "avazu/base/va.rx.app.new.csv"
    val trSiteDstPath = "avazu/base/tr.rx.site.new.csv"
    val vaSiteDstPath = "avazu/base/va.rx.site.new.csv"

    val sparkConf = new SparkConf().setAppName("GenData")
    .setMaster("local[4]")    // comment out when submitting to spark cluster

    val sc = new SparkContext(sparkConf)
    
    GenData.run(trSrcPath, vaSrcPath, trAppDstPath, vaAppDstPath, trSiteDstPath, vaSiteDstPath, sc)
//    GenData.run(avazu/tr.rx.csv avazu/va.rx.csv avazu/base/tr.rx.app.new.csv avazu/base/va.rx.app.new.csv avazu/base/tr.rx.site.new.csv avazu/base/va.rx.site.new.csv
    
    //avazu/base/tr.rx.app.new.csv avazu/base/va.rx.app.new.csv avazu/base/tr.rx.app.sp avazu/base/va.rx.app.sp
    //val trSrcPath = args(0)
    //val vaSrcPath = args(1)
    val trAppSpPath = "avazu/base/tr.rx.app.sp"
    val vaAppSpPath = "avazu/base/va.rx.app.sp"
    Converter2.convert(trAppDstPath, trAppSpPath, true, sc)
    Converter2.convert(vaAppDstPath, vaAppSpPath, false, sc)
    
    val trSiteSpPath = "avazu/base/tr.rx.site.sp"
    val vaSiteSpPath = "avazu/base/va.rx.site.sp"
    Converter2.convert(trSiteDstPath, trSiteSpPath, true, sc)
    Converter2.convert(vaSiteDstPath, vaSiteSpPath, false, sc)
    
//    cmd = './mark1 -r 0.03 -s 1 -t 13 va.r{size}.app.sp tr.r{size}.app.sp'.format(size=size) 
    
  }
}