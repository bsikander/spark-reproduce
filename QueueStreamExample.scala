package com.test

import scala.collection.mutable.Queue
import org.apache.spark.rdd.RDD
import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Try
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.spark_project.guava.io.BaseEncoding

import scala.util.control.NonFatal


/**

NOTE: In this job, awaitTermination doesn't throw exception. Even though
      I can see in SparkUI that batches are failing. Executor/driver logs
      continuously show these Job aborted exceptions but awaitTermination
      never throws.
      
      Job continues to process.

*/
object QueueStreamExample {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Streaming Executor Fail")
    val sc = new StreamingContext(sparkConf, Seconds(1))
    val results = runJob(sc)
    println("Results: " +results)
  }


  def runJob(sc: StreamingContext): Any = {
    // Create the queue through which RDDs can be pushed to
    // a QueueInputDStream
    val rddQueue = new Queue[RDD[Int]]()

    // Create the QueueInputDStream and use it do some processing
    val inputStream = sc.queueStream(rddQueue)
    val mappedStream = inputStream.map{x =>
      1/0 // ArithmeticException thrown here
      (x % 10, 1)
    }

    val reducedStream = mappedStream.reduceByKey{
      (accum, n) =>
        1/0
        (accum + n)
    }
    reducedStream.print()
    sc.start()

    // Create and push some RDDs into rddQueue
    while (true) {
      rddQueue.synchronized {
        val myrdd = sc.sparkContext.makeRDD(1 to 1000, 10)
        myrdd.setName("RDD")
        myrdd.cache()
        rddQueue += myrdd
      }
      Thread.sleep(1000)
    }

    try {
      sc.awaitTermination()
    } catch {
      case e: Throwable => println("Exception caught", e)
    }
  }
}