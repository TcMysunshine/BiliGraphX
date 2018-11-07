package com.chenhao.BiliGraphX

import com.mongodb.casbah.Imports.{MongoClient, MongoClientURI, MongoDBObject}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, GraphLoader}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object BiliAnalysis {
  def main(args: Array[String]): Unit = {
    //    屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("scalatest").setMaster("local[*]")
    val sc = new SparkContext(conf)
    // To directly connect to the default server localhost on port 27017
    val uri = MongoClientURI("mongodb://localhost:27017/")
    val mongoClient = MongoClient(uri)
    val db = mongoClient("biliSpark")
    //Query users and focus
    val users = db("users").find()
    val focus = db("focus").find()
    val vertexArr = ArrayBuffer[(Long, (String, String))]()
    val edgeArr = ArrayBuffer[Edge[Double]]()

    users.foreach(
      t => {
        //        print(t.get("id") + "->")
        //        println(t.get("name"))
        val tempVer = (t.get("id").toString.toLong, (t.get("name").toString, t.get("intro").toString))
        vertexArr += tempVer
      }
    )

    focus.foreach(
      t => {
        val tempEdge = Edge(t.get("soureId").toString.toLong, t.get("dstId").toString.toLong, 1D)
        edgeArr += tempEdge
      }
    )
    //    vertexArr.foreach(println(_))
    //    edgeArr.foreach(println(_))
    val vertexRDD: RDD[(Long, (String, String))] = sc.parallelize(vertexArr)
    val edgeRDD: RDD[Edge[Double]] = sc.parallelize(edgeArr)
    val graph: Graph[(String, String), Double] = Graph(vertexRDD, edgeRDD, ("Unknown", "Unknown"))
    //    graph.vertices.top(5).foreach(println(_))
    val ranks = graph.pageRank(0.0001).vertices
    val userVer = graph.vertices
    val rankByUser = userVer.join(ranks).map {
      case (id, (ve, rank)) => (id, ve._1, ve._2, rank)
    }
    rankByUser.sortBy(v => v._4, ascending = false).take(10).foreach(println(_))
  }

}
