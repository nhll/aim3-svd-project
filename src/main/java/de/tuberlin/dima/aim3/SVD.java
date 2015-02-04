package de.tuberlin.dima.aim3;

import de.tuberlin.dima.aim3.algorithms.Lanczos;
import de.tuberlin.dima.aim3.datatypes.Vector;
import de.tuberlin.dima.aim3.operators.MatrixReader;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

import java.io.File;
import java.util.Locale;

public class SVD {

  private static final int LANCZOS_ITERATIONS = 3;

  public static void main(String[] args) throws Exception {
    // Set default locale to US so that double values are displayed with a dot instead of a comma.
    Locale.setDefault(Locale.US);

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // Read the text file containing the input matrix line by line.
    DataSource<String> inputMatrix = env.readTextFile(SVD.class.getClassLoader().getResource("test1.matrix").toURI().toString());

    // Parse the input file to a data set of row vectors.
    DataSet<Vector> matrix = inputMatrix.map(new MatrixReader());

    // Apply the Lanczos algorithm to our input matrix and collect the result.
    Lanczos.process(matrix, LANCZOS_ITERATIONS);

    // TODO: Extract the tridiagonal matrix Tmm and the matrix containing all Lanczos vectors Vm from the result.
    // TODO: Apply the QR algorithm (?) to Tmm and collect the resulting eigenvalues and eigenvectors of Tmm.
    // TODO: Omit all of Tmm's eigenvalues that were calculated as a function of Vm's non-orthogonal Lanczos vectors.
    // TODO: Calculate A's eigenvectors by multiplying each eigenvector of Tmm with Vm.

    // --> As Tmm's eigenvalues are approximately the same as A's eigenvalues, we now have A's eigenvalues as well as
    //     A's eigenvectors.

    // Execute the whole thing!
    env.execute();
  }
}
