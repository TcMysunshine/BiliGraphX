package com.chenhao.BiliGraphX

import com.mongodb.casbah.Imports.{MongoClient, MongoClientURI}
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
//对于某一个用户找出最重要的关联用户
object personalPageRank {
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
    //init a Graph End
    //  ------------------------------------------------------------------------------------

    //  ------------------------------------------------------------------------------------
    //  个性化PageRank找出重要的关联朋友并插入数据库 Start
    val personRecCollection = db("personalRec")
    userVer.collect.foreach(t => {
      val src = t._1
      val personPageRank = graph.personalizedPageRank(src, 0.0001).vertices.filter(_._1 != src).
        reduce((a, b) => if (a._2 > b._2) a else b)
      val x = MongoDBObject("id" -> src, "recId" -> personPageRank._1)
      personRecCollection.insert(x)
    })
    //  个性化PageRank找出重要的关联朋友并插入数据库 END
    //  ------------------------------------------------------------------------------------
  }
}
