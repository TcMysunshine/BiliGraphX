package com.chenhao.BiliGraphX

import com.mongodb.casbah.Imports.{MongoClient, MongoClientURI}
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
//多个主播组成的粉丝的分布  存储在topUpFans 数据库中
object multiUp {
  def main(args: Array[String]): Unit = {
    //    屏蔽日志
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
    //init a Graph End
    val upDst = 1532165
    //    val upDst = 883968
    //    val upDst = 883968
    val edgesComu = graph.edges.filter(e => e.dstId == upDst)
    val subGraph1 = Graph.fromEdges(edgesComu, ("chen", "hao"))
    val ranKColl = db("topKUp")
    val queryRank = MongoDBObject("id" -> upDst)
    val rank = ranKColl.findOne(queryRank).get.get("rank")
    println(rank)
    subGraph1.edges.collect.foreach(t => {
      val e = MongoDBObject("Source" -> t.srcId, "Target" -> t.dstId, "Weight" -> rank, "Type" -> "Directed")
      db("topUpFans").insert(e)
    })
  }
}
