package edu.berkeley.cs.amplab.mlmatrix

import java.util.concurrent.ThreadLocalRandom
import scala.reflect.ClassTag

import breeze.linalg._

import com.github.fommil.netlib.LAPACK.{getInstance=>lapack}
import org.netlib.util.intW
import org.netlib.util.doubleW

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.rdd.RDD

class Test(
  rows: Option[Long] = None,
  cols: Option[Long] = None) extends DistributedMatrix(rows, cols) with Logging {
  override def toString = "Test"
  
  def +(other: DistributedMatrix): DistributedMatrix = ???
  def aggregateElements[U](zeroValue: U)(seqOp: (U, Double) => U,combOp: (U, U) => U)(implicit evidence$1: scala.reflect.ClassTag[U]): U = ???
  def apply(rowRange: Range,colRange: Range): DistributedMatrix = ???
  def apply(rowRange: Range,colRange: ::.type): DistributedMatrix = ???
  def apply(rowRange: ::.type,colRange: Range): DistributedMatrix = ???
  def cache(): DistributedMatrix = ???
  def collect(): DenseMatrix[Double] = ???
  protected def getDim(): (Long, Long) = ???
  def mapElements(f: Double => Double): DistributedMatrix = ???
}
