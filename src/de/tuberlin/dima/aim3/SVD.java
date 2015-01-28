package de.tuberlin.dima.aim3;

import de.tuberlin.dima.aim3.algorithms.Lanczos;
import de.tuberlin.dima.aim3.datatypes.LanczosResult;
import de.tuberlin.dima.aim3.operators.MatrixReader;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.mahout.math.MatrixWritable;

import java.io.File;

public class SVD {

  private static final int LANCZOS_ITERATIONS = 2;

  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // Read the text file containing the input matrix line by line.
    DataSource<String> inputMatrix = env.readTextFile(new File("data/test1.matrix").getAbsolutePath());

    // Parse each line of the input file to a tuple of (rowIndex, list(rowValues)).
    DataSet<Tuple2<Integer, Double[]>> matrix = inputMatrix.map(new MatrixReader());

    // Apply the Lanczos algorithm to our input matrix and collect the result.
    LanczosResult lanczosResult = Lanczos.process(matrix, LANCZOS_ITERATIONS);

    // Extract the tridiagonal matrix and the matrix containing all Lanczos vectors from the result.
    MatrixWritable Tmm = lanczosResult.getTmm();
    MatrixWritable Vm  = lanczosResult.getVm();

    // TODO: Apply the QR algorithm (?) to Tmm and collect the resulting eigenvalues and eigenvectors of Tmm.
    // TODO: Omit all of Tmm's eigenvalues that were calculated as a function of Vm's non-orthogonal Lanczos vectors.
    // TODO: Calculate A's eigenvectors by multiplying each eigenvector of Tmm with Vm.

    // --> As Tmm's eigenvalues are approximately the same as A's eigenvalues, we now have A's eigenvalues as well as
    //     A's eigenvectors.

    // Execute the whole thing!
    env.execute();
  }
}
