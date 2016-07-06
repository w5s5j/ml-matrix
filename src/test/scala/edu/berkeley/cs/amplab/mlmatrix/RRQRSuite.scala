package edu.berkeley.cs.amplab.mlmatrix

import org.scalatest.FunSuite
import org.apache.spark.SparkContext
import edu.berkeley.cs.amplab.mlmatrix.util.Utils

import breeze.linalg._
import breeze.numerics._

class RRQRSuite extends FunSuite with LocalSparkContext {

  test("Test RRQR") {
    sc = new SparkContext("local", "test")
    val A = ColPartitionedMatrix.createRandom(sc, 8, 4, 2, cache=true)
    val localA = A.collect()
    println(localA)
    
    val t = new RRQR().rrqr(A, 2)
    new RRQR().run(A)
    val tmpQRP = qrp(localA)

    println("t")
    t.foreach(println)
    println("localT")
    println(tmpQRP.q)
    println(tmpQRP.r)
    println(tmpQRP.pivotMatrix)
    println(tmpQRP.pivotIndices)
  }

}
