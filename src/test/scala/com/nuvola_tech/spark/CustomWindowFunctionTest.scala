package com.nuvola_tech.spark

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SQLContext, functions => f}
import org.scalatest.FlatSpec

class CustomWindowFunctionTest extends FlatSpec with SharedSparkContext  {
  val st = System.currentTimeMillis()
  val one_minute = 60 * 1000




  val d = Array[UserActivityData](
    UserActivityData("user1",  st, "f237e656-1e53-4a24-9ad5-2b4576a4125d"),
    UserActivityData("user2",  st +   5*one_minute, null),
    UserActivityData("user1",  st +  10*one_minute, null),
    UserActivityData("user1",  st +  15*one_minute, null),
    UserActivityData("user2",  st +  15*one_minute, null),
    UserActivityData("user1",  st + 140*one_minute, null),
    UserActivityData("user1",  st + 160*one_minute, null))

  "a CustomWindowFunction" should "correctly create a session " in {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.createDataFrame(sc.parallelize(d))

    val specs = Window.partitionBy(f.col("user")).orderBy(f.col("ts").asc)
    val res = df.withColumn( "newsession", MyUDWF.calculateSession(f.col("ts"), f.col("session")) over specs)

    df.show(20)
    res.show(20, false)

    // there should be 3 sessions
    assert( res.groupBy(f.col("newsession")).agg(f.count("newsession")).count() == 3)

  }
  it should "be able to use the window size as a parameter" in {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.createDataFrame(sc.parallelize(d))

    val specs = Window.partitionBy(f.col("user")).orderBy(f.col("ts").asc)
    val res = df.withColumn( "newsession", MyUDWF.calculateSession(f.col("ts"), f.col("session"), f.lit(one_minute)) over specs)

    df.show(20)
    res.show(20, false)

    // there should be 3 sessions
    assert( res.groupBy(f.col("newsession")).agg(f.count("newsession")).count() == 7)

  }
  it should "be able to work without initial session" in {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.createDataFrame(sc.parallelize(d.drop(1))) // drop first

    val specs = Window.partitionBy(f.col("user")).orderBy(f.col("ts").asc)
    val res = df.withColumn( "newsession", MyUDWF.calculateSession(f.col("ts"), f.col("session")) over specs)

    df.show(20)
    res.show(20, false)

    // there should be 3 sessions
    assert( res.groupBy(f.col("newsession")).agg(f.count("newsession")).count() == 3)

  }

}