package de.tuberlin.dima.aim3;

import de.tuberlin.dima.aim3.algorithms.FlinkLanczosSolver;
import de.tuberlin.dima.aim3.datatypes.Element;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.hadoopcompatibility.mapred.HadoopInputFormat;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.util.Locale;

public class SVD {

    public static void main(String[] args) throws Exception {

        // shut up flink
        //System.err.close();

        // Set default locale to US so that double values are displayed with a dot instead of a comma.
        Locale.setDefault(Locale.US);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

//        DataSet<Tuple2<Long,Long>> initialSolution = env.generateSequence(1,10).flatMap(new FlatMapFunction<Long, Tuple2<Long, Long>>() {
//            @Override
//            public void flatMap(Long value, Collector<Tuple2<Long, Long>> out) throws Exception {
//                out.collect(new Tuple2<>(value,value));
//            }
//        });
//        DataSet<Long> initialWorkset = env.fromElements(1L);
//        DeltaIteration<Long,Long> iter = initialSolution.iterateDelta(initialSolution, 1000, 1);
//        iter




        //DataSet<Element> corpus = readFromSequenceFileInputFormat(env, "/Users/fsander/Downloads/mahout-distribution-0.9/examples/bin/movielens/ratings.seq");



        DataSet<Element> A = env.fromElements(
            new Element((byte) 0, 1L, 1L, 1.0),
            new Element((byte) 0, 1L, 2L, 2.0),
            new Element((byte) 0, 1L, 3L, 3.0),
            new Element((byte) 0, 1L, 4L, 3.0),
            new Element((byte) 0, 2L, 1L, 4.0),
            new Element((byte) 0, 2L, 2L, 5.0),
            new Element((byte) 0, 2L, 3L, 6.0),
            new Element((byte) 0, 2L, 4L, 6.0),
            new Element((byte) 0, 3L, 1L, 7.0),
            new Element((byte) 0, 3L, 2L, 8.0),
            new Element((byte) 0, 3L, 3L, 9.0),
            new Element((byte) 0, 3L, 4L, 9.0)
        );

        DataSet<Element> lanzcosPlasma = FlinkLanczosSolver.solve(env, A, 4, 4, 3, false);

        lanzcosPlasma.print();

        DataSet<Element> triag = lanzcosPlasma.filter(e -> e.getId() == Config.idOfTriag);
        DataSet<Element> basis = lanzcosPlasma.filter(e -> e.getId() == Config.idOfBasis);

//        triag.print();
//
//        basis.print();

        env.execute();
    }

    private static DataSet<Element> readFromSequenceFileInputFormat(ExecutionEnvironment env, String path) {
        // hdfs integration
        JobConf job = new JobConf();
        HadoopInputFormat<Text, VectorWritable> hadoopIF =
                new HadoopInputFormat<>(new SequenceFileInputFormat<>(), Text.class, VectorWritable.class, job);
        SequenceFileInputFormat.addInputPath(job, new Path(path));
        // key needs to be row (row vectors!), value needs to be vector
        DataSet<Tuple2<Text, VectorWritable>> sequence = env.createInput(hadoopIF);
        // convert to Element
        DataSet<Element> vectors = sequence.flatMap(new FlatMapFunction<Tuple2<Text, VectorWritable>, Element>() {
            @Override
            public void flatMap(Tuple2<Text, VectorWritable> value, Collector<Element> out) throws Exception {
                Vector result = value.f1.get();
                long row = Long.parseLong(value.f0.toString());
                for (Vector.Element elem : result.nonZeroes()) {
                    long column = elem.index();
                    double val = elem.get();
                    out.collect(new Element(Config.idOfCorpus, row, column, val));
                }
            }

        });
        return vectors;
    }

}
