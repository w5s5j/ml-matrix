package edu.berkeley.cs.amplab.mlmatrix

import breeze.linalg._

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

class RRQR extends RowPartitionedSolver with Logging with Serializable {

  def qr(mat: RowPartitionedMatrix, b: Int): RowPartitionedMatrix = {
    //base on the paper
    val bt = 2 * b
    mat.cache()
    val (nRows, nCols) = mat.getDim()
    
    // TODO: need to check each partition
    //val matrixInfo = mat.rdd.map { part =>
      //(part.mat.rows.toLong, part.mat.rows.toLong)
    //}
    
    val round1 = new RowPartitionedMatrix(mat.rdd.map { part =>
      if (part.mat.cols <= b) {
        part
      } else {
        // pvt : pivot indices
        val tmpR = new TSQR().qrR(new RowPartitionedMatrix(part))
        val pvt = qr().qrp(tmpR)._4
        val getPvt = pvt.take(b)
        val getCols = getPvt.map { idx =>
          part.valueAt(idx)
        }.reduceLeft { (col1, col2) =>
          DenseMatrix.vertcat(col1, col2)
        }
        RowPartition(getCols)
      }
    })
    
    var roundN = round1

    var curCols = nCols
    while (curCols >= b) {
      var preCal = roundN.rdd.grouped(2).map { part =>
        part match {
          case (a, b) => DenseMatrix.vertcat(a.mat, b.mat) 
          case _ => _.mat
        }
      }

      roundN = new RowPartitionedMatrix(preCal.map { part =>
        if (part.cols.toLong <= b) {
          part
        } else {
          // pvt : pivot indices
          val tmpR = new TSQR.qrR(part)
          val pvt = qr.qrp(tmpR)._4
          val getPvt = pvt.take(b)
          val getCols = getPvt.map { idx =>
            part.valueAt(idx)
          }.reduceLeft { (col1, col2) =>
            DenseMatrix.vertcat(col1, col2)
          }
          getCols
        }
      })

      curCols = roundN.rdd.reduceLeft { (part1, part2) =>
        part1.mat.cols + part2.mat.cols
      }

    }

    roundN
  
  }
  
}
