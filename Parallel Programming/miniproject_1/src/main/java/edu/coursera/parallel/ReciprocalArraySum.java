package edu.coursera.parallel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;

// Class wrapping methods for implementing reciprocal array sum in parallel.
public final class ReciprocalArraySum {

  private ReciprocalArraySum() {
  }

  /**
   * Sequentially compute the sum of the reciprocal values for a given array.
   *
   * @param A Input array
   * @return The sum of the reciprocals of the array A
   */
  protected static double seqArraySum(final double[] A) {
    double sum = 0;

    // Compute sum of reciprocals of array elements
    for (int i = 0; i < A.length; ++i) {
      sum += 1 / A[i];
    }

    return sum;
  }

  /**
   * Computes the size of each chunk, given the number of chunks to create across
   * a given number of elements.
   *
   * @param nChunks   The number of chunks to create
   * @param nElements The number of elements to chunk across
   * @return The default chunk size
   */
  private static int getChunkSize(final int nChunks, final int nElements) {
    // Integer ceil
    return (nElements + nChunks - 1) / nChunks;
  }

  /**
   * Computes the inclusive element index that the provided chunk starts at, given
   * there are a certain number of chunks.
   *
   * @param chunk     The chunk to compute the start of
   * @param nChunks   The number of chunks created
   * @param nElements The number of elements to chunk across
   * @return The inclusive index that this chunk starts at in the set of nElements
   */
  private static int getChunkStartInclusive(final int chunk, final int nChunks, final int nElements) {
    final int chunkSize = getChunkSize(nChunks, nElements);
    return chunk * chunkSize;
  }

  /**
   * Computes the exclusive element index that the provided chunk ends at, given
   * there are a certain number of chunks.
   *
   * @param chunk     The chunk to compute the end of
   * @param nChunks   The number of chunks created
   * @param nElements The number of elements to chunk across
   * @return The exclusive end index for this chunk
   */
  private static int getChunkEndExclusive(final int chunk, final int nChunks, final int nElements) {
    final int chunkSize = getChunkSize(nChunks, nElements);
    final int end = (chunk + 1) * chunkSize;
    if (end > nElements) {
      return nElements;
    } else {
      return end;
    }
  }

  /**
   * This class stub can be filled in to implement the body of each task created
   * to perform reciprocal array sum in parallel.
   */
  private static class ReciprocalArraySumTask extends RecursiveAction {
    private final int lo;
    private final int hi;
    private final double[] A;
    private double value;

    private static final int SEQUENTIAL_THRESHOLD = 10000;

    /**
     * Constructor.
     *
     * @param A  Input values
     * @param lo Set the starting index to begin parallel traversal at.
     * @param hi Set ending index for parallel traversal.
     */
    ReciprocalArraySumTask(final double[] A, final int lo, final int hi) {
      this.A = A;
      this.lo = lo;
      this.hi = hi;
    }

    public double getValue() {
      return value;
    }

    @Override
    protected void compute() {
      if (hi - lo <= SEQUENTIAL_THRESHOLD) {
        // Calculate the sum sequentially
        for (int i = lo; i < hi; ++i) {
          value += 1 / A[i];
        }
      } else {
        final int mid = lo + (hi - lo) / 2;

        // Recursively call tasks
        ReciprocalArraySumTask leftSum = new ReciprocalArraySumTask(A, lo, mid);
        ReciprocalArraySumTask rightSum = new ReciprocalArraySumTask(A, mid, hi);

        // Parallelize
        leftSum.fork();
        rightSum.compute();
        leftSum.join();

        value = leftSum.value + rightSum.value;
      }
    }
  }

  /**
   * Method to compute the same reciprocal sum as seqArraySum, but use two tasks
   * running in parallel under the Java Fork Join framework. You may assume that
   * the length of the A array is evenly divisible by 2.
   *
   * @param A Input array
   * @return The sum of the reciprocals of the array A
   */
  protected static double parArraySum(final double[] A) {
    assert A.length % 2 == 0;

    ReciprocalArraySumTask task = new ReciprocalArraySumTask(A, 0, A.length);
    task.compute();
    return task.value;
  }

  /**
   * Extend the work you did to implement parArraySum to use a set number of tasks
   * to compute the reciprocal array sum. You may find the above utilities
   * getChunkStartInclusive and getChunkEtasksndExclusive helpful in computing the
   * range of element indices that belong to each chunk.
   *
   * @param A        Input array
   * @param numTasks The number of tasks to create
   * @return The sum of the reciprocals of the array A
   */
  protected static double parManyTaskArraySum(final double[] A, final int numTasks) {
    List<ReciprocalArraySumTask> tasks = new ArrayList<>();

    for (int i = 0; i < numTasks; ++i) {
      final int lo = getChunkStartInclusive(i, numTasks, A.length);
      final int hi = getChunkEndExclusive(i, numTasks, A.length);
      tasks.add(new ReciprocalArraySumTask(A, lo, hi));
    }

    ForkJoinTask.invokeAll(tasks);

    double sum = 0.0;

    for (ReciprocalArraySumTask task : tasks) {
      sum += task.getValue();
    }

    return sum;
  }
}