package com.chenhao.BiliGraphX

import com.mongodb.casbah.Imports.{MongoClient, MongoClientURI, MongoDBObject}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
//通过连通图画出不同的社区
object community {
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
    //    connectedComponents连通图 找到孤岛人群 社交圈子 start
    println("ConnectedComponents")
    val components: Graph[VertexId, Double] = graph.connectedComponents()
    //val components: Graph[VertexId, Int] = graph.stronglyConnectedComponents(10)
    val ccVertices: VertexRDD[VertexId] = components.vertices
    //    ccVertices.foreach(println(_))
    val cc = graph.vertices.join(ccVertices).map {
      case (id, (vertice, comId)) => (comId, vertice._1)
    }.groupByKey()
    //孤立人群  社交圈子
    println("不同社区中人的个数")
    println(cc.filter(x => x._2.size == 1).count())
    println(cc.filter(x => x._2.size > 1 && x._2.size < 5).count())
    println(cc.filter(x => x._2.size >= 5 && x._2.size < 10).count())
    println(cc.filter(x => x._2.size >= 10).count())
    println("社区中人数有5-20")
    val showCC = cc.filter(x => x._2.size >= 10 && x._2.size < 20).collect()
    //    showCC.foreach(t=>println(t._1.toString))
    //    val pw = new PrintWriter(new File(filepath + "/comUser1.txt"))
    showCC.foreach(t => {
      val cmId = t._1
      //社区编号
      ccVertices.collect().filter(n => n._2 == cmId).foreach(m => {
        //该社区的成员
        val src = m._1
        println(src)
        val q = MongoDBObject("Source" -> src)
        val r = db("follow").find(q)
        r.foreach(mn => {
          println(mn)
          val source = mn.get("Source")
          val target = mn.get("Target")
          val e = MongoDBObject("Source" -> source, "Target" -> target, "Type" -> "Directed")
          if (db("community").find(e).size == 0) {
            db("community").insert(e)
          }

        })
        val q1 = MongoDBObject("Target" -> src.toInt)
        val r1 = db("follow").find(q1)
        r1.foreach(mn => {
          println(mn)
          val source = mn.get("Source")
          val target = mn.get("Target")
          val e = MongoDBObject("Source" -> source, "Target" -> target, "Type" -> "Directed")
          if (db("community").find(e).size == 0) {
            db("community").insert(e)
          }
        })
      })
    })

    //    //connectedComponents连通图 找到孤岛人群 社交圈子 END
    //    ------------------------------------------------------------------------------------

  }
}
