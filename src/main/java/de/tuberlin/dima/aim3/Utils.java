package de.tuberlin.dima.aim3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by fsander on 10.02.15.
 */
public class Utils {

    public static void CustomMatrixFormatToMahoutSequenceFileFormat(int rows, int cols, String input, String output) {
        BufferedReader br = null;
        SequenceFile.Writer writer = null;

        try {
            Configuration conf = new Configuration();
            writer = new SequenceFile.Writer(FileSystem.get(conf), conf, new Path(output), IntWritable.class, VectorWritable.class);
            br = new BufferedReader(new FileReader(input));
            String s = null;
            Double[][] matrix = new Double[rows][cols];
            while ((s = br.readLine()) != null) {
                String spl[] = s.split("\\s+");
                int row = Integer.parseInt(spl[0]);
                for(int i = 1; i < spl.length; i++) {
                    int col = i-1;
                    matrix[row][col] = Double.parseDouble(spl[i]);
                }
            }
            // stub for writing. Object will be reused
            VectorWritable vec = new VectorWritable();
            for (int i = 0; i < matrix.length; i++) {
                Vector v = new RandomAccessSparseVector(matrix[i].length);
                for(int j=0; j<matrix[i].length; j++) {
                    if(matrix[i][j] != null) {
                        v.set(j, matrix[i][j]);
                    }
                }
                vec.set(v);
                writer.append(new IntWritable(i), vec);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if(br != null) {
                try {
                    br.close();
                } catch (IOException e) { }
            }
            if(writer != null) {
                try {
                    writer.close();
                } catch (IOException e) { }
            }
        }
    }

    public static void main(String[] args) {
        String input = "/Users/fsander/workspace/aim3-svd-project/src/main/resources/test1.matrix";
        String output = "/Users/fsander/workspace/aim3-svd-project/src/main/resources/test1.seq";
        Utils.CustomMatrixFormatToMahoutSequenceFileFormat(6,6, input, output);
    }



}
