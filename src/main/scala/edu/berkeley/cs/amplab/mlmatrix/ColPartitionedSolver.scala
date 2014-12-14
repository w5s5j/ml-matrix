package edu.berkeley.cs.amplab.mlmatrix

import breeze.linalg._

import org.apache.spark.rdd.RDD

abstract class ColPartitionedSolver {

  def solveLeastSquares(
      A: ColPartitionedMatrix,
      b: ColPartitionedMatrix): DenseMatrix[Double] = {
    solveLeastSquaresWithL2(A, b, 0.0)
  }

  def solveLeastSquaresWithL2(
      A: ColPartitionedMatrix,
      b: ColPartitionedMatrix,
      lambda: Double): DenseMatrix[Double] = {
    solveLeastSquaresWithManyL2(A, b, Array(lambda)).head
  }

   /**
    * Solves a single least squares problem with l2 regularization using
    * several different lambdas as regularization parameters
    */
  def solveLeastSquaresWithManyL2(
      A: ColPartitionedMatrix,
      b: ColPartitionedMatrix,
      lambdas: Array[Double]): Seq[DenseMatrix[Double]]

   /**
    * Solves several least squares problems (by varying the b vector and keeping
    * the A matrix constant) with l2 regularization using different lambdas as
    * regularization parameters.
    */
  // TODO: This interface should ideally take in Seq[ColPartitionedMatrix] ?
  def solveManyLeastSquaresWithL2(
      A: ColPartitionedMatrix,
      b: RDD[Seq[DenseMatrix[Double]]],
      lambdas: Array[Double]): Seq[DenseMatrix[Double]]

}
