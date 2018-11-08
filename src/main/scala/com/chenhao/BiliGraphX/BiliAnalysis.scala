package com.chenhao.BiliGraphX

import com.mongodb.casbah.Imports.{MongoClient, MongoClientURI, MongoDBObject}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.graphstream.graph.implementations.{AbstractEdge, SingleGraph, SingleNode}
import java.io.PrintWriter
import scala.collection.mutable.ArrayBuffer
import java.io.File

object BiliAnalysis {
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
//  ------------------------------------------------------------------------------------

//  ------------------------------------------------------------------------------------
//  个性化PageRank找出重要的关联朋友并插入数据库 Start
//    val personRecCollection = db("personalRec")
//    userVer.collect.foreach(t=>{
//      val src = t._1
//      val personPageRank = graph.personalizedPageRank(src,0.0001).vertices.filter(_._1!=src).
//        reduce((a,b)=>if(a._2>b._2) a else b)
//      val x = MongoDBObject("id"->src,"recId"->personPageRank._1)
//      personRecCollection.insert(x)
//    })
//  个性化PageRank找出重要的关联朋友并插入数据库 END
//  ------------------------------------------------------------------------------------


//    val src = 883968
//    val personPageRank = graph.personalizedPageRank(src,0.0001).vertices.filter(_._1!=src).
//       reduce((a,b)=>if(a._2>b._2) a else b)
//    println(personPageRank._1.toString)


//    ------------------------------------------------------------------------------------
//     Get TopKFans and TopKUps Start
//  前十个粉丝最多的
//    println("前K个粉丝最多的Up主")
////    val K = 10
//    val topFans = graph.inDegrees
//    val topKFansCollection = db("topKFans")
//    val rankByFans = userVer.join(topFans)
//    val topKFans = rankByFans.sortBy(t=>t._2._2,ascending = false).collect()
//    topKFans.foreach(t=>{
//      val x = MongoDBObject("id"->t._1,"name"->t._2._1._1,"intro"->t._2._1._2,"fans"->t._2._2)
//      topKFansCollection.insert(x)
//    })
//    topKFans.foreach(println(_))
//    println("前十个最有影响力的up主")
////  前K个最有影响力的up主
//    val ranks = graph.pageRank(0.0001).vertices
//    val rankByUser = userVer.join(ranks)
//    val topKUp = rankByUser.sortBy(v => v._2._2, ascending = false).collect()
////  将前几个最受欢迎的推送到数据库
//    val topKUpCollection = db("topKUp")
//    topKUp.foreach(t=>{
//      val x = MongoDBObject("id"->t._1,"name"->t._2._1._1,"intro"->t._2._1._2,"rank"->t._2._2)
//      topKUpCollection.insert(x)
//    })
//    topKUp.foreach(println(_))
//     Get TopKFans and TopKUps END
    // ------------------------------------------------------------------------------------

//    ------------------------------------------------------------------------------------
//    connectedComponents连通图 找到孤岛人群 社交圈子 start
    println("ConnectedComponents")
    val components: Graph[VertexId, Double] = graph.connectedComponents()
    //val components: Graph[VertexId, Int] = graph.stronglyConnectedComponents(10)
    val ccVertices: VertexRDD[VertexId] = components.vertices
//    ccVertices.foreach(println(_))
    val cc = graph.vertices.join(ccVertices).map{
      case (id,(vertice,comId))=>(comId,vertice._1)
    }.groupByKey()
    //孤立人群  社交圈子
    println("不同社区中人的个数")
    println(cc.filter(x=>x._2.size==1).count())
    println(cc.filter(x=>x._2.size>1&&x._2.size<5).count())
    println(cc.filter(x=>x._2.size>=5&&x._2.size<10).count())
    println(cc.filter(x=>x._2.size>=10).count())
    println("社区中人数有5-10")
    val showCC = cc.filter(x=>x._2.size>=10&&x._2.size<20).collect()
//    showCC.foreach(t=>println(t._1.toString))
//    val pw = new PrintWriter(new File(filepath + "/comUser1.txt"))
    showCC.foreach(t=>{
      val cmId = t._1
//      println("------社区ID"+cmId.toString)
      ccVertices.collect().filter(n=>n._2==cmId).foreach(m=>{
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
//        pw.write(src.toString + "\n")
      })
    })

//    //connectedComponents连通图 找到孤岛人群 社交圈子 END
//    ------------------------------------------------------------------------------------

    //Community 图 插入Mongo Start
//    val x = sc.textFile(filepath + "/comUser.txt")
//    x.collect.foreach(m => {
//      println(m)
//      val q = MongoDBObject("Source" -> m.toInt)
//      val r = db("follow").find(q)
//      r.foreach(mn => {
//        println(mn)
//        val e = MongoDBObject("Source" -> mn.get("Source"), "Target" -> mn.get("Target"), "Type"->"Directed")
//        db("comu").insert(e)
//      })
//
//      val q1 = MongoDBObject("Target" -> m.toInt)
//      val r1 = db("follow").find(q1)
//      r1.foreach(mn => {
//        println(mn)
//        val e = MongoDBObject("Source" -> mn.get("Source"), "Target" -> mn.get("Target"), "Type"->"Directed")
//        db("comu").insert(e)
//      })
//    })
    //Community 图  插入Mongo END
//    val upDst = 1532165
//    val upDst = 883968
//    val upDst = 883968
//    val edgesComu = graph.edges.filter(e => e.dstId== upDst)
//    val subGraph1 = Graph.fromEdges(edgesComu,("chen","hao"))
//    val ranKColl = db("topKUp")
//    val queryRank = MongoDBObject("id"->upDst)
//    val rank = ranKColl.findOne(queryRank).get.get("rank")
//    println(rank)
//    subGraph1.edges.collect.foreach(t=>{
//      val e = MongoDBObject("Source"->t.srcId,"Target"->t.dstId,"Weight"->rank,"Type"->"Directed")
//      db("topUpFans").insert(e)
//    })
//    ------------------------------------------------------------------------------------
    //画出某一个Up主的粉丝最多的图Start
//    val upDst = 122879
//    val upDst = 13934589
//    val edgesComu = graph.edges.filter(e => e.dstId== upDst)
//    val subGraph1 = Graph.fromEdges(edgesComu,("chen","hao"))
//    val num:Int = (edgesComu.count() * 0.1).toInt
//    val fansOfFile = filepath + "/fansOf"+upDst.toString+".txt"
//    val pw =new PrintWriter(fansOfFile)
//    subGraph1.edges.take(num).foreach(t=>{
//      pw.write(t.srcId.toString + "\n")
//    })
//    pw.close()
//    val x = sc.textFile(fansOfFile)
//    val fansOfColl = db("fansOf")
//    x.collect.foreach(m => {
//      println(m)
//      val q = MongoDBObject("Source" -> m.toInt)
//      val r = db("follow").find(q)
//      r.foreach(mn => {
//        println(mn)
//        val source = mn.get("Source")
//        val target = mn.get("Target")
//        val e = MongoDBObject("Source" -> source, "Target" -> target, "Type" -> "Directed")
//        if(fansOfColl.find(e).size==0){
//          fansOfColl.insert(e)
//        }
//
//      })
//      val q1 = MongoDBObject("Target" -> m.toInt)
//      val r1 = db("follow").find(q1)
//      r1.foreach(mn => {
//        println(mn)
//        val source = mn.get("Source")
//        val target = mn.get("Target")
//        val e = MongoDBObject("Source" -> source, "Target" -> target, "Type" -> "Directed")
//        if(fansOfColl.find(e).size==0){
//          fansOfColl.insert(e)
//        }
//      })
//    })

//    drawGraphX(subGraph1)
    //画出前十粉丝数最多的图END
//    ------------------------------------------------------------------------------------


//    cc.filter(x=>x._2.size==1).map(t=>t._1+"->"+t._2.mkString(",")).foreach(println(_))
//     connectedComponents连通图 END
//    ------------------------------------------------------------------------------------
    // LPA 社区发现算法 Start
//    println("-------LPA社区发现算法--------")
//    val lpaGraph = lib.LabelPropagation.run(graph, 40)
//    val lpaVertices = lpaGraph.vertices
//    val communities = graph.vertices.join(lpaVertices).map{
//      case(id, (vertice, label))=> (label,id)
//    }.groupByKey()
//    //社区人数超过十的才成为一个社区
////    communities.filter(x=>x._2.size>10).map(t=>(t._1,t._2.size)).foreach(println(_))
//    communities.filter(x=>x._2.size>10).map(t=> t._1+"->"+t._2.mkString(",")).foreach(println(_))
    // LPA 社区发现算法 END
//    ------------------------------------------------------------------------------------
  }



  //画图函数
  def drawGraphX(srcGraph:Graph[(String, String), Double]):Unit = {
    val graph: SingleGraph = new SingleGraph("graphDemo")
    graph.setAttribute("ui.stylesheet", "url(file:/Users/chenhao/Documents/Data/stylesheet)")
    graph.setAttribute("ui.quality")
    graph.setAttribute("ui.antialias")

    //    load the graphx vertices into GraphStream
    for ((id, _) <- srcGraph.vertices.collect()){
      val node = graph.addNode(id.toString).asInstanceOf[SingleNode]
    }

    //    load the graphx edges into GraphStream edges
    for (Edge(x, y, _) <- srcGraph.edges.collect()) {
      val edge = graph.addEdge(x.toString ++ y.toString, x.toString, y.toString, true).asInstanceOf[AbstractEdge]
    }
    import scala.collection.JavaConversions._
    //显示标签
    for (node <- graph) {
      //      println(node.getLabel("label"))
      node.addAttribute("ui.label", node.getId)
      node.setAttribute("ui.class", "marked")
    }
    //    graph display
    val viewer = graph.display()
  }
  //最大出入度函数中的reduce函数
  def max(a:(VertexId,Int),b:(VertexId,Int)):(VertexId,Int)={
    if(a._2>b._2) a else b
  }
  //构造图
  def getGraph(sc: SparkContext):Graph[(String, String), Double] ={
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
    //init a Graph
    val vertexRDD: RDD[(Long, (String, String))] = sc.parallelize(vertexArr)
    val edgeRDD: RDD[Edge[Double]] = sc.parallelize(edgeArr)
    val graph: Graph[(String, String), Double] = Graph(vertexRDD, edgeRDD, ("Unknown", "Unknown"))
    return graph
  }
}
