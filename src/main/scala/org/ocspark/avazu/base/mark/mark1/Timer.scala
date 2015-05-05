package org.ocspark.avazu.base.mark.mark1

import java.util.Date

object Timer {
  var begin = new Date()
  var duration = 0L;

  def main(args: Array[String]): Unit = {
    reset();
  }

  def reset() {
    begin = new Date()
    duration = 0

  }

  def tic() {
    begin = new Date()
  }

  def toc(): Long = {
    duration += (new Date().getTime() - begin.getTime());
    return duration / 1000;
  }

  def get(): Long = {
    val time = toc();
    tic();
    time;
  }

}