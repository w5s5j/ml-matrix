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

  def run(matrix: ColPartitionedMatrix): Int = {
    matrix.cache()
    val (nRows, nCols) = matrix.getDim()
    val nPart = matrix.rdd.map{_ => 1}.reduce(_ + _)
    val b = nCols / nPart

    val round1Sele = rrqr(matrix, b.toInt).sorted
    val round1Swap = round1Sele.zip(0 to round1Sele.length - 1)
    round1Swap.foreach { swap =>
      val tmp = matrix(::, swap._1 to swap._1).rdd.take(1).mat.toDenseVector
      matrix(::, swap._1) = matrix(::, swap._2 to swap._2).rdd.take(1).mat.toDenseVector
      matrix(::, swap._2) = tmp
    }
    println(matrix.collect)

    1
  
  }


  // b is the number of selected col
  def rrqr(matrix: ColPartitionedMatrix, b: Int): Array[Int] = {
    matrix.cache()
    val (nRows, nCols) = matrix.getDim()
    
    // TODO: need to check each partition
    //val matrixInfo = mat.rdd.map { part =>
      //(part.mat.rows.toLong, part.mat.rows.toLong)
    //}
    
    val colsPerPartition = matrix.rdd.zipWithIndex.map { case (part, idx) =>
      (idx, part.mat)
    }
    //flatten all cols
    val colsInfo = colsPerPartition.flatMap { case (idx, denMat) =>
      (0 to denMat.cols - 1).map{ colId =>
        (idx, colId, denMat(::, colId))
      }
    }

    val blocksPerPartition = colsPerPartition.mapValues(_.cols).collect().toMap
    val partitionBlockStart = new collection.mutable.HashMap[Long, Int]
    partitionBlockStart.put(0, 0)
    (1 until matrix.rdd.partitions.size).foreach { p =>
      partitionBlockStart(p) =
        blocksPerPartition.getOrElse(p - 1, 0) + partitionBlockStart(p - 1)
    }
    // colWithIndex = RDD[(DenseVector, Int/Long)]
    val colsWithIndex = colsInfo.map { x =>
      (x._3, partitionBlockStart(x._1) + x._2)
    }

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
    //roundN.map { part =>
      //part.mat
    //}.reduce(DenseMatrix.horzcat(_, _))
    val selectCols = roundN.flatMap { part =>
      val colPart = part.mat
      (0 to colPart.cols - 1).map { id =>
        colPart(::, id)
      }
    }.collect()

    selectCols.map { col =>
      colsWithIndex.lookup(col)(0)
    }
  }

  def solveLeastSquaresWithManyL2(A: ColPartitionedMatrix, b: ColPartitionedMatrix,lambdas: Array[Double]): Seq[DenseMatrix[Double]] = ???
  
  def solveManyLeastSquaresWithL2(A: ColPartitionedMatrix, b: RDD[Seq[DenseMatrix[Double]]], lambdas: Array[Double]): Seq[DenseMatrix[Double]] = ???
}
