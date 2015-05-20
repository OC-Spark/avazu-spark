package org.ocspark.avazu.bag.util

import org.ocspark.avazu.Common
import org.apache.spark.SparkContext

object CatId {

  def main(args: Array[String]): Unit = {

  }

  def run(sp: String, out: String, submit: String, sc: SparkContext) {
    val id = sc.textFile(sp, 4).map {
      line =>
        line.split(" ", 2)(0)
    }
    val click = sc.textFile(out).map {
      clickStr =>
        clickStr.toDouble
    }

    val idClick = Common.genPerEventPrediction(id, click).map {
      id_click =>
        id_click._1 + "," + id_click._2
    }
    Common.writeOut(idClick.collect.mkString("\n"), submit)
  }

}