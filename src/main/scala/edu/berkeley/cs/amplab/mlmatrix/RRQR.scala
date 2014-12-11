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

  def rrqr(sc: SparkContext, mat: RowPartitionedMatrix, b: Int): DenseMatrix[Double] = {
    //base on the paper
    val bt = 2 * b
    mat.cache()
    val (nRows, nCols) = mat.getDim()
    
    // TODO: need to check each partition
    //val matrixInfo = mat.rdd.map { part =>
      //(part.mat.rows.toLong, part.mat.rows.toLong)
    //}
    
    val round1 = mat.rdd.map { part =>
      if (part.mat.cols <= b) {
        part
      } else {
        // pvt : pivot indices
        val tmpR = new TSQR().qrR(RowPartitionedMatrix.fromArray(sc.parallelize(Seq(part.mat.data)), Seq.fill(1)(part.mat.rows), part.mat.cols))
        val tmpQRP = qrp(tmpR)
        val getPvt = tmpQRP.pivotIndices.take(b)
        val getCols = getPvt.map { idx =>
          part.mat(::, idx).toDenseMatrix
        }.reduceLeft { (col1, col2) =>
          DenseMatrix.vertcat(col1, col2)
        }
        RowPartition(getCols)
      }
    }
    
    var roundN = round1

    var curCols = nCols
    while (curCols > b) {

      val g1 = roundN.zipWithIndex

      var preCal = roundN.grouped(2).map { part =>
        part match {
          case (a, b) => DenseMatrix.vertcat(a.mat, b.mat) 
          case _ => _.mat
        }
      }

      roundN = preCal.map { part =>
        if (part.cols.toLong <= b) {
          part
        } else {
          // pvt : pivot indices
          val tmpR = new TSQR().qrR(RowPartitionedMatrix.fromArray(sc.parallelize(Seq(part.mat.data)), Seq.fill(1)(part.mat.rows), part.mat.cols))
          val tmpQRP = qrp(tmpR)
          val getPvt = tmpQRP.pivotIndices.take(b)
          val getCols = getPvt.map { idx =>
            part.mat(::, idx).toDenseMatrix
          }.reduceLeft { (col1, col2) =>
            DenseMatrix.vertcat(col1, col2)
          }
          getCols
        }
      }

      curCols = roundN.map { part => 
        part.mat.cols.toInt 
      }.reduce(_ + _)


    }

    roundN.collect().map { part =>
      part.mat
    }.reduceLeft { (col1, col2) =>
      DenseMatrix.vertcat(col1, col2)
    }
      
  }
  
}
