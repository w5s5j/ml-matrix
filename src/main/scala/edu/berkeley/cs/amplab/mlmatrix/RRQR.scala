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
  def rrqr(sc: SparkContext, mat: RowPartitionedMatrix, b: Int): DenseMatrix[Double] = {
    //base on the paper
    val bt = 2 * b
    mat.cache()
    val (nRows, nCols) = mat.getDim()
    
    // TODO: need to check each partition
    //val matrixInfo = mat.rdd.map { part =>
      //(part.mat.rows.toLong, part.mat.rows.toLong)
    //}
    
    // Compute the first round, do TSQR and get just R on each partition
    // for partition which number of column less than b, stay the same
    // round1 is RDD[RowPartition]
    // mat.rdd is RDD[RowPartition]
    val round1 = mat.rdd.map { part =>
      if (part.mat.cols <= b) {
        part
      } else {
        // Do TSQR to get just R
        val tmpR = new TSQR().qrR(RowPartitionedMatrix.fromArray(sc.parallelize(Seq(part.mat.data)), Seq.fill(1)(part.mat.rows), part.mat.cols))
        // pvt : pivot indices
        // Do RRQR on the R just get 
        // and take the first b elements in pivot to form the matrix 
        // based on the original Partition
        val getPvt = qrp(tmpR).pivotIndices.take(b)
        RowPartition(getPvt.map { idx =>
          part.mat(::, idx).toDenseMatrix
        }.reduceLeft { (col1, col2) =>
          DenseMatrix.vertcat(col1, col2)
        })
      }
    }
    
    var roundN = round1
    var curCols = nCols
    // TODO: Naive inplement for tree reduction, need to be improve
    while (curCols > b) {
      // group roundN, two elements in the same group and vertcat them
      // eg. [1, 2, 3, 4, 5, 6] => [vertcat(1, 2), vertcat(3, 4), vertcat(5, 6)]
      //     [1, 2, 3, 4, 5] => [vertcat(1, 2), vertcat(3, 4), 5]
      // preCal is RDD[DenseMatrix]
      val g1 = roundN.zipWithIndex.filter(_._2 % 2  == 0)
      val g2 = roundN.zipWithIndex.filter(_._2 % 2  == 1)
      var preCal = g1.zipPartitions(g2)(comb)
      // roundN is basically the same as round1 except it starts from a RDD[DenseMatrix]
      roundN = preCal.map { part =>
        if (part.cols.toLong <= b) {
          RowPartition(part)
        } else {
          // pvt : pivot indices
          val tmpR = new TSQR().qrR(RowPartitionedMatrix.fromArray(sc.parallelize(Seq(part.data)), Seq.fill(1)(part.rows), part.cols))
          val tmpQRP = qrp(tmpR)
          val getPvt = tmpQRP.pivotIndices.take(b)
          val getCols = getPvt.map { idx =>
            part(::, idx).toDenseMatrix
          }.reduceLeft { (col1, col2) =>
            DenseMatrix.vertcat(col1, col2)
          }
          RowPartition(getCols)
        }
      }

      // Compute the total number of cols in each iteration, needed to decide whether
      // to continue the reduction
      curCols = roundN.map { part => 
        part.mat.cols.toInt 
      }.reduce(_ + _)
    }

    // return a dense matrix, vertcat all RowPartition
    roundN.collect().map { part =>
      part.mat
    }.reduceLeft { (col1, col2) =>
      DenseMatrix.vertcat(col1, col2)
    }
  }

  //helper function for combine two Iter
  private def comb(aiter: Iterator[(RowPartition, Long)],
           biter: Iterator[(RowPartition, Long)])
          :Iterator[DenseMatrix[Double]] = {
    var res = List[DenseMatrix[Double]]()
    while (aiter.hasNext && biter.hasNext) {
      val x = DenseMatrix.vertcat(aiter.next._1.mat, biter.next._1.mat)
      res ::= x
    }
    if (aiter.hasNext) {
      res ::= aiter.next._1.mat
    } else if (biter.hasNext) {
      res ::= biter.next._1.mat
    }
    res.iterator
  }
  
  def solveLeastSquaresWithManyL2(A: RowPartitionedMatrix, b: RowPartitionedMatrix,lambdas: Array[Double]): Seq[DenseMatrix[Double]] = ???
  
  def solveManyLeastSquaresWithL2(A: RowPartitionedMatrix, b: RDD[Seq[DenseMatrix[Double]]], lambdas: Array[Double]): Seq[DenseMatrix[Double]] = ???
}
