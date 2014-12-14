package edu.berkeley.cs.amplab.mlmatrix

import java.util.concurrent.ThreadLocalRandom
import scala.reflect.ClassTag

import breeze.linalg._

import com.github.fommil.netlib.LAPACK.{getInstance=>lapack}
import org.netlib.util.intW
import org.netlib.util.doubleW

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.rdd.RDD

/** Note: [[breeze.linalg.DenseMatrix]] by default uses column-major layout. */
case class ColPartition(mat: DenseMatrix[Double]) extends Serializable
case class ColPartitionInfo(
  partitionId: Int, // RDD partition this block is in
  blockId: Int, // BlockId goes from 0 to numBlocks
  startRow: Long) extends Serializable

class ColPartitionedMatrix(
  val rdd: RDD[ColPartition],
  rows: Option[Long] = None,
  cols: Option[Long] = None) extends DistributedMatrix(rows, cols) with Logging {

  // Map from partitionId to ColPartitionInfo
  // Each RDD partition can have multiple ColPartition
  @transient var partitionInfo_ : Map[Int, Array[ColPartitionInfo]] = null

  override def getDim() = {
    val dims = rdd.map { lm =>
      (lm.mat.rows.toLong, lm.mat.cols.toLong)
    }.reduce { case(a, b) =>
      (a._1, a._2 + b._2)
    }
    dims
  }

  private def calculatePartitionInfo() {
    // Partition information sorted by (partitionId, matrixInPartition)
    val rowsPerPartition = rdd.mapPartitionsWithIndex { case (part, iter) =>
      if (iter.isEmpty) {
        Iterator()
      } else {
        iter.zipWithIndex.map(x => (part, x._2, x._1.mat.rows.toLong))
      }
    }.collect().sortBy(x => (x._1, x._2))

    // TODO(shivaram): Test this and make it simpler ?
    val blocksPerPartition = rowsPerPartition.groupBy(x => x._1).mapValues(_.length)

    val partitionBlockStart = new collection.mutable.HashMap[Int, Int]
    partitionBlockStart.put(0, 0)
    (1 until rdd.partitions.size).foreach { p =>
      partitionBlockStart(p) =
        blocksPerPartition.getOrElse(p - 1, 0) + partitionBlockStart(p - 1)
    }

    val rowsWithblockIds = rowsPerPartition.map { x =>
      (x._1, partitionBlockStart(x._1) + x._2, x._3)
    }

    val cumulativeSum = rowsWithblockIds.scanLeft(0L){ case (x1, x2) =>
      x1 + x2._3
    }.dropRight(1)

    partitionInfo_ = rowsWithblockIds.map(x => (x._1, x._2)).zip(
      cumulativeSum).map(x => ColPartitionInfo(x._1._1, x._1._2, x._2)).groupBy(x => x.partitionId)
  }

  def getPartitionInfo = {
    if (partitionInfo_ == null) {
      calculatePartitionInfo()
    }
    partitionInfo_
  }

  override def +(other: Double) = {
    new ColPartitionedMatrix(rdd.map { lm =>
      ColPartition(lm.mat :+ other)
    }, rows, cols)
  }

  override def *(other: Double) = {
    new ColPartitionedMatrix(rdd.map { lm =>
      ColPartition(lm.mat :* other)
    }, rows, cols)
  }

  override def mapElements(f: Double => Double) = {
    new ColPartitionedMatrix(rdd.map { lm =>
      ColPartition(
        new DenseMatrix[Double](lm.mat.rows, lm.mat.cols, lm.mat.data.map(f)))
    }, rows, cols)
  }

  override def aggregateElements[U: ClassTag](zeroValue: U)(seqOp: (U, Double) => U, combOp: (U, U) => U): U = {
    rdd.map { part =>
      part.mat.data.aggregate(zeroValue)(seqOp, combOp)
    }.reduce(combOp)
  }

  override def reduceRowElements(f: (Double, Double) => Double): DistributedMatrix = {
    val reducedRows = rdd.map { rowPart =>
      // get row-major layout by transposing
      val rows = rowPart.mat.data.grouped(rowPart.mat.rows).toSeq.transpose
      val reduced = rows.map(_.reduce(f)).toArray
      ColPartition(new DenseMatrix[Double](rowPart.mat.rows, 1, reduced))
    }
    new ColPartitionedMatrix(reducedRows, rows, Some(1))
  }

  override def reduceColElements(f: (Double, Double) => Double): DistributedMatrix = {
    val reducedColsPerPart = rdd.map { rowPart =>
      val cols = rowPart.mat.data.grouped(rowPart.mat.rows)
      cols.map(_.reduce(f)).toArray
    }
    val collapsed = reducedColsPerPart.reduce { case arrPair => arrPair.zipped.map(f) }
    ColPartitionedMatrix.fromArray(
      rdd.sparkContext.parallelize(Seq(collapsed), 1), Seq(1), numCols().toInt)
  }

  override def +(other: DistributedMatrix) = {
    other match {
      case otherBlocked: ColPartitionedMatrix =>
        if (this.dim == other.dim) {
          // Check if matrices share same partitioner and can be zipped
          if (rdd.partitions.size == otherBlocked.rdd.partitions.size) {
            new ColPartitionedMatrix(rdd.zip(otherBlocked.rdd).map { case (lm, otherLM) =>
              ColPartition(lm.mat :+ otherLM.mat)
            }, rows, cols)
          } else {
            throw new SparkException(
              "Cannot add matrices with unequal partitions")
          }
        } else {
          throw new IllegalArgumentException("Cannot add matrices of unequal size")
        }
      case _ =>
        throw new IllegalArgumentException("Cannot add matrices of different types")
    }
  }

  override def apply(rowRange: Range, colRange: ::.type) = {
    this.apply(rowRange, Range(0, numCols().toInt))
  }

  override def apply(rowRange: ::.type, colRange: Range) = {
    new ColPartitionedMatrix(rdd.map { lm =>
      ColPartition(lm.mat(::, colRange))
    })
  }

  override def apply(rowRange: Range, colRange: Range) = {
    // TODO: Make this a class member
    val partitionBroadcast = rdd.sparkContext.broadcast(getPartitionInfo)

    // First filter partitions which have rows in this index, then select them
    ColPartitionedMatrix.fromMatrix(rdd.mapPartitionsWithIndex { case (part, iter) =>
      if (partitionBroadcast.value.contains(part)) {
        val startRows = partitionBroadcast.value(part).sortBy(x => x.blockId).map(x => x.startRow)
        iter.zip(startRows.iterator).flatMap { case (lm, sr) =>
          // TODO: Handle Longs vs. Ints correctly here
          val matRange = sr.toInt until (sr.toInt + lm.mat.rows)
          if (matRange.contains(rowRange.start) || rowRange.contains(sr.toInt)) {
            // The end row is min of number of rows in this partition
            // and number of rows left to read
            val start = (math.max(rowRange.start - sr, 0)).toInt
            val end = (math.min(rowRange.end - sr, lm.mat.rows)).toInt
            Iterator(lm.mat(start until end, colRange))
          } else {
            Iterator()
          }
        }
      } else {
        Iterator()
      }
    })
  }

  override def cache() = {
    rdd.cache()
    this
  }

  // TODO: This is terribly inefficient if we have more partitions.
  // Make this more efficient
  override def collect(): DenseMatrix[Double] = {
    val parts = rdd.map(x => x.mat).collect()
    parts.reduceLeftOption((a,b) => DenseMatrix.horzcat(a, b)).getOrElse(new DenseMatrix[Double](0, 0))
  }

  // Apply a function to each partition of the matrix
  def mapPartitions(f: DenseMatrix[Double] => DenseMatrix[Double]) = {
    // TODO: This can be efficient if we don't change num rows per partition
    ColPartitionedMatrix.fromMatrix(rdd.map { lm =>
      f(lm.mat)
    })
  }
}

object ColPartitionedMatrix {

  // Convert an RDD[DenseMatrix[Double]] to an RDD[ColPartition]
  def fromMatrix(matrixRDD: RDD[DenseMatrix[Double]]): ColPartitionedMatrix = {
    new ColPartitionedMatrix(matrixRDD.map(mat => ColPartition(mat)))
  }

  def fromArray(matrixRDD: RDD[Array[Double]]): ColPartitionedMatrix = {
    fromMatrix(arrayToMatrix(matrixRDD))
  }

  def fromArray(
      matrixRDD: RDD[Array[Double]],
      rowsPerPartition: Seq[Int],
      cols: Int): ColPartitionedMatrix = {
    new ColPartitionedMatrix(
      arrayToMatrix(matrixRDD, rowsPerPartition, cols).map(mat => ColPartition(mat)),
        Some(rowsPerPartition.sum), Some(cols))
  }

  def arrayToMatrix(
      matrixRDD: RDD[Array[Double]],
      rowsPerPartition: Seq[Int],
      cols: Int) = {
    val rBroadcast = matrixRDD.context.broadcast(rowsPerPartition)
    val data = matrixRDD.mapPartitionsWithIndex { case (part, iter) =>
      val rows = rBroadcast.value(part)
      val matData = new Array[Double](rows * cols)
      var nRow = 0
      while (iter.hasNext) {
        val arr = iter.next()
        var idx = 0
        while (idx < arr.size) {
          matData(nRow + idx * rows) = arr(idx)
          idx = idx + 1
        }
        nRow += 1
      }
      Iterator(new DenseMatrix[Double](rows, cols, matData.toArray))
    }
    data
  }

  def arrayToMatrix(matrixRDD: RDD[Array[Double]]): RDD[DenseMatrix[Double]] = {
    val rowsColsPerPartition = matrixRDD.mapPartitionsWithIndex { case (part, iter) =>
      if (iter.hasNext) {
        val nCols = iter.next().size
        Iterator((part, 1 + iter.size, nCols))
      } else {
        Iterator((part, 0, 0))
      }
    }.collect().sortBy(x => (x._1, x._2, x._3)).map(x => (x._1, (x._2, x._3))).toMap

    val rBroadcast = matrixRDD.context.broadcast(rowsColsPerPartition)

    val data = matrixRDD.mapPartitionsWithIndex { case (part, iter) =>
      val (rows, cols) = rBroadcast.value(part)
      val matData = new Array[Double](rows * cols)
      var nRow = 0
      while (iter.hasNext) {
        val arr = iter.next()
        var idx = 0
        while (idx < arr.size) {
          matData(nRow + idx * rows) = arr(idx)
          idx = idx + 1
        }
        nRow += 1
      }
      Iterator(new DenseMatrix[Double](rows, cols, matData.toArray))
    }
    data
  }

  def createRandom(sc: SparkContext,
      numRows: Int,
      numCols: Int,
      numParts: Int,
      cache: Boolean = true): ColPartitionedMatrix = {
    val colsPerPart = numCols / numParts
    val matrixParts = sc.parallelize(1 to numParts, numParts).mapPartitions { part =>
      val data = new Array[Double](colsPerPart * numRows)
      var i = 0
      while (i < colsPerPart*numRows) {
        data(i) = ThreadLocalRandom.current().nextDouble()
        i = i + 1
      }
      val mat = new DenseMatrix[Double](numRows, colsPerPart, data)
      Iterator(mat)
    }
    if (cache) {
      matrixParts.cache()
    }
    ColPartitionedMatrix.fromMatrix(matrixParts)
  }

}
