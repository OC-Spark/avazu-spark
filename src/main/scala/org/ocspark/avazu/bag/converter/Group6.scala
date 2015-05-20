package org.ocspark.avazu.bag.converter

import breeze.linalg.max
import scala.annotation.meta.param
import scala.annotation.meta.param
import scopt.OptionParser
import scala.annotation.meta.param
import breeze.util.partition
import scala.annotation.meta.param
import scala.annotation.meta.param
import org.ocspark.avazu.AbstractParams
import java.util.Date
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.ocspark.avazu.base.util.GenData
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.HashMap
import org.apache.spark.sql.GroupedData
import breeze.macros.expand.args
import breeze.macros.expand.args
import org.ocspark.avazu.util.Counter
import breeze.macros.expand.args
import breeze.macros.expand.args
import breeze.macros.expand.args
import org.apache.spark.mllib.feature.HashingTF
import breeze.macros.expand.args
import breeze.macros.expand.args
import java.io.PrintWriter
import java.io.File
import org.ocspark.avazu.Common

object Group6 {
  val nr_range = 1000000
  val tf = new HashingTF(nr_range)

  val f_fields = Array[String]("hour", "banner_pos", "device_id", "device_ip", "device_model", "device_conn_type", "C14", "C17", "C20", "C21", "pub_id", "pub_domain", "pub_category", "device_id_count", "device_ip_count", "user_count", "smooth_user_hour_count", "user_click_histroy")

  def vtform(v: String, partition: String, k: String, cnts: Map[String, Map[String, Int]], max_occur: Int): String = { // k = pub_id or pub_domain
    var returnString = ""
    val pub_in_raw = collection.immutable.HashMap("pub_id" -> collection.immutable.HashMap("app" -> "app_id", "site" -> "site_id"),
      "pub_domain" -> collection.immutable.HashMap("app" -> "app_domain", "site" -> "site_domain"),
      "pub_category" -> collection.immutable.HashMap("app" -> "app_category", "site" -> "site_category"))
    var c = k
    if (pub_in_raw.contains(k)) {
      pub_in_raw.get(k) match {
        case Some(m) => c = m.getOrElse(partition, "")
        case None => // do nothing
      }
    }
    if (c == "hour") {
      returnString = c + "-" + v.substring(0, v.length - 2)
    } else {
      val cntsc = cnts.getOrElse(c, Map[String, Int]())
      if (cntsc.contains(v)) {
        var valueCount = 0
        cntsc.get(v) match {
          case Some(n) => valueCount = n
          case None => // do nothing
        }
        if (valueCount >= max_occur) {		// max_occur defaults to 100
          returnString = c + "-" + v
        } else {
          returnString = c + "-less-" + valueCount		// only use count feature if smaller than threshold
        }
      } else {
        returnString = c + "-less"
      }
    }
    returnString
  }

  def generate_feats(trva2: RDD[Array[String]], partition: String, a_field: String, gc_begin: Int, max_occur: Int, max_sz_group: Int, max_nr_group_feats: Int, groupCounts: Map[String, Map[String, Int]], pidIndex: Int): RDD[(String, String)] = {
    val g_added = (a_field.split(',').toSet & f_fields.toSet).toList.sorted
    val col_fm_indices = new collection.mutable.HashMap[String, Int]()
    val kidIndex = pidIndex - 1
    val srcIndex = kidIndex - 1

    var i = 0
    g_added.foreach {
      c =>
        col_fm_indices.put(c, i + gc_begin)
        i += 1
    }
    val pidGroup = trva2.groupBy(a => a(pidIndex))
    println("pidGroup")
    //    pidGroup.collect.foreach(p => println(p))
    println("pidGroup size = " + pidGroup.count)
    val feat_strs = pidGroup.map {

      k_iterable =>
        var train_feats_str = new ArrayBuffer[String]()
        val va_feats_str = new ArrayBuffer[String]()
        val iterable = k_iterable._2
        val kidGroup = iterable.groupBy(itr => itr(kidIndex))		// kid is pub_id-pub_domain
        println("kidgroup size = " + kidGroup.keys.size)
        kidGroup.foreach {
          k_v =>
            val key = k_v._1
            val vIterable = k_v._2
            var group_feats = new collection.mutable.HashMap[String, Map[String, Double]]
            val count = vIterable.count(i => true)
            var gf_strs = new ArrayBuffer[String]()
            if (count < max_sz_group) { // max_sz_group default is 100
              for (c <- g_added) {
                val vtAb = new ArrayBuffer[String]()
                vIterable.foreach {
                  row =>
                    val v = row(GenData.newFieldMap(c))
                    val vtString = vtform(v.toString, partition, c, groupCounts, max_occur)
                    vtAb.append(vtString)
                }

                val counter = new Counter(vtAb.toArray) // counts freq of a generated vt string
                var sum = 0
                counter.items.foreach {
                  k_c =>
                    val count = k_c._2
                    val countSql = Math.pow(count, 2).toInt
                    sum += countSql
                }
                val sqrt = Math.sqrt(sum)
                val c_norm = 1 / sqrt / g_added.size
                counter.items.foreach {
                  v_c =>
                    val vt = v_c._1
                    val w = v_c._2
                    val nw = w.toDouble * c_norm
                    group_feats.put(c, Map[String, Double](vt -> nw))
                }
              } //  for (c <- g_added) 
              group_feats.foreach {
                feat =>
                  val c = feat._1
                  val vws = feat._2
                  vws.foreach {
                    vw =>
                      val vt = vw._1
                      val w = vw._2
                      val col_fm_index = col_fm_indices.getOrElse(c, 0)
                      val hashIndex = tf.indexOf("group-" + vt)
                      gf_strs.append(f"$col_fm_index:$hashIndex:$w%2.5f")
                  }
              }
            } // if (count < max_sz_group)
            val gf_str = gf_strs.toArray.mkString(" ")
            vIterable.foreach {
              row =>
                val rowId = row(GenData.newFieldMap("id"))
                val feats_str = rowId + gf_str
                if (row(srcIndex) == "train") {
                  train_feats_str.append(feats_str)
                } else {
                  va_feats_str.append(feats_str)
                }

            }

        } // kidGroup.foreach {
        (train_feats_str.mkString("\n"), va_feats_str.mkString("\n"))
    } // val feat_strs = pidGroup.map
    feat_strs
  }

  def genKid(g_field: String, words: Array[String]): String = {
    var kid = ""
    if (g_field != "device_id") {
      val kidWords = new ArrayBuffer[String]()
      g_field.split(",").foreach(w => kidWords.append(words(GenData.newFieldMap(w))))
      kid = kidWords.toArray.mkString("-")
    } else {
      if (words(GenData.newFieldMap("device_id")) != "a99f214a") {
        kid = words(GenData.newFieldMap("device_id"))
      } else {
        kid = words(GenData.newFieldMap("device_ip")) + "-" + words(GenData.newFieldMap("device_model"))
      }
    }
    //    println("kid = " + kid)
    kid
  }

  def get_pid_table(wordsRdd: RDD[Array[String]], col: String, sz_chunk: Long, kidIndex: Int): collection.mutable.Map[String, Int] = {
    //    wordsRdd.collect.foreach(l => println(l.length))
    val ccMap = new collection.mutable.HashMap[String, Int]()
    val idIndex = GenData.newFieldMap("id")
    wordsRdd.map {
      words =>
        //        println("words length = " + words.length)
        val kid = words(kidIndex)
        val idValue = words(idIndex)
        (kid, idValue)
    }.groupByKey()
      .map {
        kv =>
          val count = kv._2.size
          (kv._1, count)
      }.collect()
      .foreach {
        var cc = 0
        kc =>
          val key = kc._1
          val count = kc._2
          cc += count
          ccMap.put(key, cc)
      }
    ccMap
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Group6")
      .setMaster("local[4]") // comment out when submitting to spark cluster

    val sc = new SparkContext(sparkConf)

    val defaultParams = Group6Params()

    val parser = new OptionParser[Group6Params]("SparseNaiveBayes") {
      head("SparseNaiveBayes: an example naive Bayes app for LIBSVM data.")
      opt[Int]("gc_begin")
        .text("the index of the first column in group features ")
        .action((x, c) => c.copy(gc_begin = x))
      opt[Int]("max_occur")
        .text("specified the maximum number of count features. Any feature with counts less than the value would be replaced with its count.")
        .action((x, c) => c.copy(max_occur = x))
      opt[Int]("max_sz_group")
        .text(s"the upper limit of the size of each group, default: 100")
        .action((x, c) => c.copy(max_sz_group = x))
      opt[Int]("max_nr_group_feats")
        .text(s"the maximum number of features among a group")
        .action((x, c) => c.copy(max_sz_group = x))
      arg[String]("train")
        .text("csv file")
        .required()
        .action((x, c) => c.copy(train = x))
      arg[String]("valid")
        .text("csv file")
        .required()
        .action((x, c) => c.copy(valid = x))
      arg[String]("partition")
        .text("site/app")
        .required()
        .action((x, c) => c.copy(partition = x))
      arg[String]("g_field")
        .text("specified the fields used to group instances")
        .required()
        .action((x, c) => c.copy(g_field = x))
      arg[String]("a_field")
        .text("specified the fields considered in each group")
        .required()
        .action((x, c) => c.copy(a_field = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params, Map[String, Map[String, Int]](), sc)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Group6Params, groupCounts: Map[String, Map[String, Int]], sc: SparkContext) {
//    val spec = s".T_$params.max_occur.gins_$params.max_sz_group.gfeat_$params.max_nr_group_feats.gby_$params.g_field.add_$params.a_field"		// not used because no need for tmp files
//    println("Group6.run: train = " + params.train + ", valid=" + params.valid )
    //  # loading
    var start = new Date()
    val tr = sc.textFile("hdfs://localhost" + params.train, 4)
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map {
        line =>
          val words = line.split(",", -1)

          words ++ Array("train")
      }

    val srcIndex = tr.take(1)(0).length - 1
    println("srcIndex = " + srcIndex)

    val va = sc.textFile("hdfs://localhost" + params.valid, 4)
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map {
        line =>
          val words = line.split(",", -1)

          words ++ Array("valid")
      }

    //  va["__src__"] = "__va__"
    val trva = tr.union(va)
    //    println("trva length")
    //    trva.collect.foreach( t => println(t.length))
    val g_field = params.g_field
    println("trva count = " + trva.count)
    val kidIndex = srcIndex + 1
    var i = 0
    val trva1 = trva.map {
      words =>
        val kid = genKid(g_field, words)
        i += 1
        words ++ Array(kid)
    }

    println("Loading: %s sec.".format(new Date().getTime() - start.getTime()) * 1000)

    //  # assign process IDs
    start = new Date()
    val sz_chunk = Math.max(20000, (trva.count / 100) + 1)
    val ccMap = get_pid_table(trva1, "__kid__", sz_chunk, kidIndex)
    val trva2: RDD[Array[String]] = trva1.map {
      words =>
        val idVal = words(GenData.newFieldMap("id"))
        val cc = ccMap.getOrElse(idVal, 0)
        words ++ Array(cc + "")
    }

    val pidIndex = kidIndex + 1

    print("Compute the sizes of groups: %s sec.".format(new Date().getTime() - start.getTime()))

    //  # compute group features in parallel
    start = new Date()

    val trva_files = generate_feats(trva2, params.partition, params.a_field, params.gc_begin, params.max_occur, params.max_sz_group, params.max_nr_group_feats, groupCounts, pidIndex)

    val sb1 = new ArrayBuffer[String]()
    val sb2 = new ArrayBuffer[String]()
    val collected_file_strs = trva_files.collect.foreach {
      strTuple =>
        sb1.append(strTuple._1)
        sb2.append(strTuple._2)
    }

    Common.writeOut(sb1.mkString(""), params.train.replace("/base", "/bag") + ".group")
    Common.writeOut(sb2.mkString(""), params.valid.replace("/base", "/bag") + ".group")

    print("Calculate groups features: %s sec.".format(new Date().getTime() - start.getTime()))

  }

}