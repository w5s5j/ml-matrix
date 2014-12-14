package edu.berkeley.cs.amplab.mlmatrix

import breeze.linalg._
import breeze.numerics._

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import edu.berkeley.cs.amplab.mlmatrix.util.QRUtils
import edu.berkeley.cs.amplab.mlmatrix.util.Utils

class RRQR extends RowPartitionedSolver with Logging with Serializable {

  // b is the number of selected col
  def rrqr(matrix: RowPartitionedMatrix, b: Int): DenseMatrix[Double] = {
    //base on the paper
    val bt = 2 * b
    matrix.cache()
    val (nRows, nCols) = matrix.getDim()
    val calTSQR = new TSQR()
    
    // TODO: need to check each partition
    //val matrixInfo = mat.rdd.map { part =>
      //(part.mat.rows.toLong, part.mat.rows.toLong)
    //}
    
    println("s1")
    // Compute the first round, do TSQR and get just R on each partition
    // for partition which number of column less than b, stay the same
    // round1 is RDD[RowPartition]
    // mat.rdd is RDD[RowPartition]
    val round1 = matrix.rdd.map { part =>
      if (part.mat.cols <= b) {
        part
      } else {
        // Do TSQR to get just R
        val tmpR = calTSQR.qrR(RowPartitionedMatrix.fromArray(matrix.rdd.sparkContext.parallelize(Seq(part.mat.data), 1), Seq.fill(1)(part.mat.rows), part.mat.cols))
        val rowsPerPart = part.mat.rows.toInt / 4
        val prePart = Seq(part.mat(0 * rowsPerPart to 1 * rowsPerPart - 1, ::),
                          part.mat(1 * rowsPerPart to 2 * rowsPerPart - 1, ::),
                          part.mat(2 * rowsPerPart to 3 * rowsPerPart - 1, ::),
                          part.mat(3 * rowsPerPart to part.mat.rows.toInt - 1, ::))
        val sc = new SparkContext("local", "test")
        val tmpR = new TSQR().qrR(new RowPartitionedMatrix(
          sc.makeRDD(prePart.map(RowPartition(_)))
        ))
        //val tmpR = QRUtils.qrR(part.mat)
        // pvt : pivot indices
        // Do RRQR on the R just get 
        // and take the first b elements in pivot to form the matrix 
        // based on the original Partition
        val getPvt = qrp(tmpR).pivotIndices.take(b)
        RowPartition(getPvt.map { idx =>
          part.mat(::, idx).toDenseMatrix.t
        }.reduce(DenseMatrix.horzcat(_, _)))
      }
    }
    
    var roundN = round1

    println("s2")
    var curCols = round1.map { part =>
      part.mat.cols.toInt
    }.reduce(_ + _)

    // TODO: Naive inplement for tree reduction, need to be improve
    println(b)
    //while (curCols > b*2) {
    while (curCols > b) {


      println("loop")
      println(roundN)
      println(curCols)
      // group roundN, two elements in the same group and vertcat them
      // eg. [1, 2, 3, 4, 5, 6] => [vertcat(1, 2), vertcat(3, 4), vertcat(5, 6)]
      //     [1, 2, 3, 4, 5] => [vertcat(1, 2), vertcat(3, 4), 5]
      // preCal is RDD[DenseMatrix]
      var preCal = roundN.zipWithIndex.map { part =>
        (part._2 / 2, part._1.mat)
      }.reduceByKey(DenseMatrix.horzcat(_, _)).map(_._2)

      // roundN is basically the same as round1 except it starts from a RDD[DenseMatrix]
      roundN = preCal.map { part =>
        if (part.cols.toInt <= b) {
          RowPartition(part)
        } else {
          // pvt : pivot indices
          //val tmpR = calTSQR.qrR(RowPartitionedMatrix(matrix.rdd.sparkContext.parallelize(Seq(part.data), 1), Seq.fill(1)(part.rows), part.cols))
          val rowsPerPart = part.rows.toInt / 4
          println(111)
          val prePart = Seq(part(0 * rowsPerPart to 1 * rowsPerPart - 1, ::),
                            part(1 * rowsPerPart to 2 * rowsPerPart - 1, ::),
                            part(2 * rowsPerPart to 3 * rowsPerPart - 1, ::),
                            part(3 * rowsPerPart to part.rows.toInt - 1, ::))
          println(111)
          val preComp = prePart.map(RowPartition(_))
          println(111)
          val sc = new SparkContext("local", "test")
          val preRDD = sc.makeRDD(preComp)
          //val preRDD = matrix.rdd.sparkContext.makeRDD(preComp)
          println(111)
          val tmpR = new TSQR().qrR(new RowPartitionedMatrix(preRDD))
          println(111)
          //val tmpR = QRUtils.qrR(part)
          val tmpQRP = qrp(tmpR)
          val getPvt = tmpQRP.pivotIndices.take(b)
          val getCols = getPvt.map { idx =>
            part(::, idx).toDenseMatrix.t
          }.reduce(DenseMatrix.horzcat(_, _))
          RowPartition(getCols)
        }
      }

      // Compute the total number of cols in each iteration, needed to decide whether
      // to continue the reduction
      curCols = roundN.map { part =>
        part.mat.cols.toInt 
      }.reduce(_ + _)

      println(roundN.map { part =>
        part.mat
      }.reduce(DenseMatrix.horzcat(_, _)))

    }

    println("gothrough")
    // return a dense matrix, vertcat all RowPartition
    roundN.map { part =>
      part.mat
    }.reduce(DenseMatrix.horzcat(_, _))
  }

  def solveLeastSquaresWithManyL2(A: RowPartitionedMatrix, b: RowPartitionedMatrix,lambdas: Array[Double]): Seq[DenseMatrix[Double]] = ???
  
  def solveManyLeastSquaresWithL2(A: RowPartitionedMatrix, b: RDD[Seq[DenseMatrix[Double]]], lambdas: Array[Double]): Seq[DenseMatrix[Double]] = ???
}
