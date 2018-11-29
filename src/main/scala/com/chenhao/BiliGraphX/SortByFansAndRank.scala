package com.chenhao.BiliGraphX

import com.mongodb.casbah.Imports.{MongoClient, MongoClientURI, MongoDBObject}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.graphstream.graph.implementations.{AbstractEdge, SingleGraph, SingleNode}
import java.io.PrintWriter
import scala.collection.mutable.ArrayBuffer
//根据粉丝和up主影响力的排名
object SortByFansAndRank {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val filepath = "/Users/chenhao/Documents/Data/GraphxData/bili"
    val conf = new SparkConf().setAppName("scalatest").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val uri = MongoClientURI("mongodb://localhost:27017/")
    val mongoClient = MongoClient(uri)
    val db = mongoClient("biliSparkSmall")
    //Query users and focus
    val users = db("users").find()
    val focus = db("follow").find()
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
        val tempEdge = Edge(t.get("Source").toString.toLong, t.get("Target").toString.toLong, 1D)
        edgeArr += tempEdge
      }
    )
    //    ------------------------------------------------------------------------------------
    //    init a Graph Start
    val vertexRDD: RDD[(Long, (String, String))] = sc.parallelize(vertexArr)
    val edgeRDD: RDD[Edge[Double]] = sc.parallelize(edgeArr)
    val graph: Graph[(String, String), Double] = Graph(vertexRDD, edgeRDD, ("Unknown", "Unknown"))
    val userVer = graph.vertices
    //    ------------------------------------------------------------------------------------
    //     Get TopKFans and TopKUps Start
    //  前十个粉丝最多的
    println("按照粉丝数量排序的Up主")
    val K = 10
    val topFans = graph.inDegrees
    val topKFansCollection = db("topKFans")
    val rankByFans = userVer.join(topFans)
    val topKFans = rankByFans.sortBy(t => t._2._2, ascending = false).collect()
    topKFans.foreach(t => {
      val x = MongoDBObject("id" -> t._1, "name" -> t._2._1._1, "intro" -> t._2._1._2, "fans" -> t._2._2)
      topKFansCollection.insert(x)
    })
    topKFans.foreach(println(_))
    println("按照影响力排序的up主")
    //  前K个最有影响力的up主
    val ranks = graph.pageRank(0.0001).vertices
    val rankByUser = userVer.join(ranks)
    val topKUp = rankByUser.sortBy(v => v._2._2, ascending = false).collect()
    //  将前几个最受欢迎的推送到数据库
    val topKUpCollection = db("topKUp")
    topKUp.foreach(t => {
      val x = MongoDBObject("id" -> t._1, "name" -> t._2._1._1, "intro" -> t._2._1._2, "rank" -> t._2._2)
      topKUpCollection.insert(x)
    })
    topKUp.foreach(println(_))
    //         Get TopKFans and TopKUps END
    // ------------------------------------------------------------------------------------
  }
}
