package de.tuberlin.dima.aim3;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import de.tuberlin.dima.aim3.algorithms.Lanczos;
import de.tuberlin.dima.aim3.datatypes.LanczosResult;
import de.tuberlin.dima.aim3.datatypes.Vector;
import de.tuberlin.dima.aim3.operators.MatrixReader;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.function.PlusMult;
import org.apache.mahout.math.solver.EigenDecomposition;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Locale;
import java.util.Map;

public class SVD {

    private static final int LANCZOS_ITERATIONS = 3;

    public static void main(String[] args) throws Exception {
        // Set default locale to US so that double values are displayed with a dot instead of a comma.
        Locale.setDefault(Locale.US);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Read the text file containing the input matrix line by line.
        DataSource<String> inputMatrix =
                env.readTextFile(SVD.class.getClassLoader().getResource("test1.matrix").toURI().toString());

        // Parse the input file to a data set of row vectors.
        DataSet<Vector> matrix = inputMatrix.map(new MatrixReader());

        // Apply the Lanczos algorithm to our input matrix and collect the result.
        LanczosResult lanczosResult = Lanczos.process(matrix, LANCZOS_ITERATIONS);

        DataSet<Vector> Tmm = lanczosResult.getTmm();
        DataSet<Vector> Vm = lanczosResult.getVm();

        Tmm.writeAsText(Config.pathToTmm(), FileSystem.WriteMode.OVERWRITE);
        Vm.writeAsText(Config.pathToVm(), FileSystem.WriteMode.OVERWRITE);

        // TODO: Extract the tridiagonal matrix Tmm and the matrix containing all Lanczos vectors Vm from the result.
        // TODO: Apply the QR algorithm (?) to Tmm and collect the resulting eigenvalues and eigenvectors of Tmm.
        // TODO: Omit all of Tmm's eigenvalues that were calculated as a function of Vm's non-orthogonal Lanczos vectors.
        // TODO: Calculate A's eigenvectors by multiplying each eigenvector of Tmm with Vm.

        // --> As Tmm's eigenvalues are approximately the same as A's eigenvalues, we now have A's eigenvalues as well as
        //     A's eigenvectors.

        env.execute();

        testResult(true);
    }

    private static void testResult(boolean isSymmetric) {
        try {
            Map<Integer, org.apache.mahout.math.Vector> singularVectors = Maps.newHashMap();
            Map<Integer, Double> singularValues = Maps.newHashMap();

            String TmmString = readMatrix(Config.pathToTmm());
            String VmString = readMatrix(Config.pathToVm());

            Matrix Tmm = parseMatrix(TmmString);
            Matrix Vm = parseMatrix(VmString);

            EigenDecomposition decomp = new EigenDecomposition(Tmm);
            Matrix eigenVects = decomp.getV();
            org.apache.mahout.math.Vector eigenVals = decomp.getRealEigenvalues();

            for (int row = 0; row < eigenVects.columnSize(); row++) {
                org.apache.mahout.math.Vector realEigen = null;

                org.apache.mahout.math.Vector ejCol = eigenVects.viewColumn(row);
                int size = Math.min(ejCol.size(), Vm.rowSize());
                for (int j = 0; j < size; j++) {
                    double d = ejCol.get(j);
                    // In our implementation, V vectors are stored as row vectors...
                    org.apache.mahout.math.Vector rowJ = Vm.viewRow(j);
                    if (realEigen == null) {
                        realEigen = rowJ.like();
                    }
                    realEigen.assign(rowJ, new PlusMult(d));
                }
                Preconditions.checkState(realEigen != null);
                assert realEigen != null;
                realEigen = realEigen.normalize();

                singularVectors.put(row, realEigen);

                double scaleFactor = Vm.viewRow(2).norm(2);
                double e = eigenVals.get(row) * scaleFactor;

                if (!isSymmetric) {
                    e = Math.sqrt(e);
                }

                System.out.println("Eigenvector " + row + " found with eigenvalue " + e);
                singularValues.put(row, e);
            }

            System.out.println(singularVectors.toString());
            System.out.println(singularValues.toString());
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    private static Matrix parseMatrix(String matrixString) {
        String[] rows = matrixString.split("\n");
        int rowSize = rows.length;
        int colSize = rows[0].split("\\s+").length - 1;
        double[][] matrixElements = new double[rowSize][colSize];

        for (String row : rows) {
            String[] tokens = row.split("\\s+");
            int rowIndex = new Integer(tokens[0]);
            for (int col = 1; col < tokens.length; col++) {
                matrixElements[rowIndex][col - 1] = new Double(tokens[col]);
            }
        }

        return new DenseMatrix(matrixElements);
    }

    private static String readMatrix (String path) throws Exception {
        String result = "";
        File file = new File(path);
        if (file.isFile()) {
            result = readFile(path);
        } else if (file.isDirectory()) {
            for (final File fileEntry : file.listFiles()) {
                result = result.concat(readFile(fileEntry.getAbsolutePath()));
            }
        }
        return result;
    }

    private static String readFile(String path) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(path));
        StringBuilder sb = new StringBuilder();
        String line = br.readLine();
        while (line != null) {
            sb.append(line);
            sb.append(System.lineSeparator());
            line = br.readLine();
        }
        return sb.toString();
    }
}
