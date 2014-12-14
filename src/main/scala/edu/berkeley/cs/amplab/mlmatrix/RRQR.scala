package edu.berkeley.cs.amplab.mlmatrix

import breeze.linalg._
import breeze.numerics._

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import edu.berkeley.cs.amplab.mlmatrix.util.QRUtils
import edu.berkeley.cs.amplab.mlmatrix.util.Utils

class RRQR extends ColPartitionedSolver with Logging with Serializable {

  // b is the number of selected col
  def rrqr(matrix: ColPartitionedMatrix, b: Int): DenseMatrix[Double] = {
    //base on the paper
    val bt = 2 * b
    matrix.cache()
    val (nRows, nCols) = matrix.getDim()
    
    // TODO: need to check each partition
    //val matrixInfo = mat.rdd.map { part =>
      //(part.mat.rows.toLong, part.mat.rows.toLong)
    //}
    
    // Compute the first round, do TSQR and get just R on each partition
    // for partition which number of column less than b, stay the same
    // round1 is RDD[ColPartition]
    // mat.rdd is RDD[ColPartition]
    val round1 = matrix.rdd.map { part =>
      if (part.mat.cols <= b) {
        part
      } else {
        // Do TSQR to get just R
        val rowsPerPart = part.mat.rows.toInt / 4
        val prePart = Seq(part.mat(0 * rowsPerPart to 1 * rowsPerPart - 1, ::),
                          part.mat(1 * rowsPerPart to 2 * rowsPerPart - 1, ::),
                          part.mat(2 * rowsPerPart to 3 * rowsPerPart - 1, ::),
                          part.mat(3 * rowsPerPart to part.mat.rows.toInt - 1, ::))
        val sc = new SparkContext("local", "test")
        val tmpR = new TSQR().qrR(new RowPartitionedMatrix(
          sc.makeRDD(prePart.map(RowPartition(_)), 2)
        ))
        // pvt : pivot indices
        // Do RRQR on the R just get 
        // and take the first b elements in pivot to form the matrix 
        // based on the original Partition
        val getPvt = qrp(tmpR).pivotIndices.take(b)
        ColPartition(getPvt.map { idx =>
          part.mat(::, idx).toDenseMatrix.t
        }.reduce(DenseMatrix.horzcat(_, _)))
      }
    }
    
    var roundN = round1

    var curCols = round1.map { part =>
      part.mat.cols.toInt
    }.reduce(_ + _)

    // TODO: Naive inplement for tree reduction, need to be improve
    while (curCols > b) {


      // group roundN, two elements in the same group and horzcat them
      // eg. [1, 2, 3, 4, 5, 6] => [horzcat(1, 2), horzcat(3, 4), horzcat(5, 6)]
      //     [1, 2, 3, 4, 5] => [horzcat(1, 2), horzcat(3, 4), 5]
      // preCal is RDD[DenseMatrix]
      var preCal = roundN.zipWithIndex.map { part =>
        (part._2 / 2, part._1.mat)
      }.reduceByKey(DenseMatrix.horzcat(_, _)).map(_._2)

      // roundN is basically the same as round1 except it starts from a RDD[DenseMatrix]
      roundN = preCal.map { part =>
        if (part.cols.toInt <= b) {
          ColPartition(part)
        } else {
          // pvt : pivot indices
          val rowsPerPart = part.rows.toInt / 4
          val prePart = Seq(part(0 * rowsPerPart to 1 * rowsPerPart - 1, ::),
                            part(1 * rowsPerPart to 2 * rowsPerPart - 1, ::),
                            part(2 * rowsPerPart to 3 * rowsPerPart - 1, ::),
                            part(3 * rowsPerPart to part.rows.toInt - 1, ::))
          val sc = new SparkContext("local", "test")
          val tmpR = new TSQR().qrR(new RowPartitionedMatrix(
            sc.makeRDD(prePart.map(RowPartition(_)), 2)
          ))
          val tmpQRP = qrp(tmpR)
          val getPvt = tmpQRP.pivotIndices.take(b)
          val getCols = getPvt.map { idx =>
            part(::, idx).toDenseMatrix.t
          }.reduce(DenseMatrix.horzcat(_, _))
          ColPartition(getCols)
        }
      }

      // Compute the total number of cols in each iteration, needed to decide whether
      // to continue the reduction
      curCols = roundN.map { part =>
        part.mat.cols.toInt 
      }.reduce(_ + _)

    }

    // return a dense matrix, horzcat all ColPartition
    roundN.map { part =>
      part.mat
    }.reduce(DenseMatrix.horzcat(_, _))
  }

  def solveLeastSquaresWithManyL2(A: ColPartitionedMatrix, b: ColPartitionedMatrix,lambdas: Array[Double]): Seq[DenseMatrix[Double]] = ???
  
  def solveManyLeastSquaresWithL2(A: ColPartitionedMatrix, b: RDD[Seq[DenseMatrix[Double]]], lambdas: Array[Double]): Seq[DenseMatrix[Double]] = ???
}
