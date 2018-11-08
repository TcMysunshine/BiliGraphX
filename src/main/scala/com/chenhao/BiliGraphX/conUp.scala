package com.chenhao.BiliGraphX

import java.io.PrintWriter

import com.mongodb.casbah.Imports.{MongoClient, MongoClientURI}
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
//关注该主播的所有粉丝的分布关注点和被关注 并存储在 Mongo fansOf 中
object conUp {
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
    //    ------------------------------------------------------------------------------------
    //画出某一个Up主的粉丝最多的图Start
    //        val upDst = 122879
    val upDst = 13934589
    val edgesComu = graph.edges.filter(e => e.dstId == upDst)
    val subGraph1 = Graph.fromEdges(edgesComu, ("chen", "hao"))
    val num: Int = (edgesComu.count() * 0.1).toInt
    val fansOfFile = filepath + "/fansOf" + upDst.toString + ".txt"
    val pw = new PrintWriter(fansOfFile)
    subGraph1.edges.take(num).foreach(t => {
      pw.write(t.srcId.toString + "\n")
    })
    pw.close()
    val x = sc.textFile(fansOfFile)
    val fansOfColl = db("fansOf")
    x.collect.foreach(m => {
      println(m)
      val q = MongoDBObject("Source" -> m.toInt)
      val r = db("follow").find(q)
      r.foreach(mn => {
        println(mn)
        val source = mn.get("Source")
        val target = mn.get("Target")
        val e = MongoDBObject("Source" -> source, "Target" -> target, "Type" -> "Directed")
        if (fansOfColl.find(e).size == 0) {
          fansOfColl.insert(e)
        }

      })
      val q1 = MongoDBObject("Target" -> m.toInt)
      val r1 = db("follow").find(q1)
      r1.foreach(mn => {
        println(mn)
        val source = mn.get("Source")
        val target = mn.get("Target")
        val e = MongoDBObject("Source" -> source, "Target" -> target, "Type" -> "Directed")
        if (fansOfColl.find(e).size == 0) {
          fansOfColl.insert(e)
        }
      })
    })

  }
}
