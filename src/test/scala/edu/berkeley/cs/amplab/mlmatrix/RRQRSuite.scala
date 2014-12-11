package edu.berkeley.cs.amplab.mlmatrix

import org.scalatest.FunSuite
import org.apache.spark.SparkContext
import edu.berkeley.cs.amplab.mlmatrix.util.Utils

import breeze.linalg._
import breeze.numerics._

class RRQRSuite extends FunSuite with LocalSparkContext {

  test("Test RRQR") {
    sc = new SparkContext("local", "test")
    val A = RowPartitionedMatrix.createRandom(sc, 8, 4, 2, cache=true)
    val localA = A.collect()
    
    val t = new RRQR().rrqr(sc, A, 2)
    val tmpQRP = qrp(localA)
    val localT = tmpQRP.pivotIndices.take(2)

    println("t")
    println(t)
    println("localT")
    println(localT)
  }

}
