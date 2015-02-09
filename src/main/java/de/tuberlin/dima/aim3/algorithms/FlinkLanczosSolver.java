package de.tuberlin.dima.aim3.algorithms;

import de.tuberlin.dima.aim3.Config;
import de.tuberlin.dima.aim3.datatypes.Element;
import de.tuberlin.dima.aim3.operators.Multiplication;
import de.tuberlin.dima.aim3.operators.custom.*;
import de.tuberlin.dima.aim3.operators.extended.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.mahout.math.decomposer.lanczos.LanczosSolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FlinkLanczosSolver extends LanczosSolver {

    private static final Logger log = LoggerFactory.getLogger(FlinkLanczosSolver.class);

    private static DataSet<Element> getInitialBaseVector(ExecutionEnvironment env, long desiredNumRows) {
        return env.generateSequence(1, desiredNumRows).map(new MapFunction<Long, Element>() {
            @Override
            public Element map(Long row) throws Exception {
                return new Element(Config.idOfBasis, row, 1L, Math.sqrt(1.0 / (double) desiredNumRows));
            }
        });
    }

    public static DataSet<Element> solve(ExecutionEnvironment env, DataSet<Element> corpus, long numRows, long numCols, int desiredRank, boolean isSymmetric) {
        return solve(env, corpus, numRows, numCols, desiredRank, isSymmetric, null);
    }

    public static DataSet<Element> solve(ExecutionEnvironment env, DataSet<Element> corpus, long numRows, long numCols, int desiredRank, boolean isSymmetric, Double scaleFactor) {

        // we actually want to compute eigenpairs of A^T * T, but if A is symmetric, this equals to A with each element
        // squared. So in that case, we leave A untouched, and sqrt the eigenvalues of A^T * T in the unsymmetric case
        // to get same results as in the symmetric case
        if (!isSymmetric) {
            corpus = Multiplication.multiplySquared(corpus, corpus, Config.idOfCorpus);
        }
        /*
        - each delta iteration has two inputs: workSet and solutionSet
        - each iteration produces two results: new workSet and solutionSet-delta
        - a delta iteration is initialized on the initial solutionSet
        - arguments to that are the initial deltaSet, maxIterations and key positions
         */

        /*
            ### pre-step ###
         */
        DataSet<Element> v1 = getInitialBaseVector(env, numRows);
        if(desiredRank == 1) {
            return finalizeLanczos(corpus, v1, desiredRank, isSymmetric);
        }
        DataSet<Element> v2 = Multiplication.multiply(corpus, v1, Config.idOfBasis).runOperation(new IncrementColumn());

        DataSet<Double> scaleFactorSet;
        if(scaleFactor != null && scaleFactor > 0.0) {
            scaleFactorSet = env.fromElements(1.0 / scaleFactor);
        }
        else {
            scaleFactorSet = v2.runOperation(new GetScaleFactor());
        }
        v2 = Multiplication.scalarDouble(v2, scaleFactorSet);
        DataSet<Element> alpha1 = Multiplication.dot(v1, v2, Config.idOfTriag).runOperation(new SetRowAndCol(1L, 1L));
        //if(1==1) return alpha1;
        // v2 -= alpha * v1. This step is the actual orthogonalization (why?). Alpha contains the "un-orthogonal"
        // projection. This is why we don't need to orthogonalize the nextVector to the previous. It has been done as part
        // of the algorithm.
        v2 = Multiplication.substract(v2, Multiplication.scalar(v1, alpha1));
        // in case of puzzlement about beta**2**: in tridiag we have 1 to m alphas (diagonal) and 2 to m betas (one off diagonal)
        DataSet<Element> beta2 = v2.runOperation(new CreateElementWithL2Norm(Config.idOfTriag)).runOperation(new SetRowAndCol(2L, 1L));

        if(desiredRank == 2) {
            return finalizeLanczos(corpus, v1.union(v2.union(beta2.union(alpha1))), desiredRank, isSymmetric);
        }

        /*
            ### initialize iteration ###
         */
        // the solution set contains elements, that we don't need in an interation step. For now, this is only the first alpha
        DataSet<Element> initialSolutionSet = alpha1;
        // the workset contains all the stuff that is needed to proccess on that. That is v_i-1, v_i, and b_i
        DataSet<Element> initialWorkingset = v1.union(v2.union(beta2));
        DeltaIteration<Element, Element> iteration =
                initialSolutionSet.iterateDelta(initialWorkingset, desiredRank - 2, Element.ID, Element.ROW, Element.COL);
        /*
            ### iteration step ###
         */
        // get needed stuff from workset
        DataSet<Element> workset = iteration.getWorkset();
        // get v_i
        DataSet<Element> currentVector = workset.runOperation(new FilterCurrentVector());
        // get v_i-1 / will be empty in first iteration
        DataSet<Element> previousVector = workset.runOperation(new FilterPreviousVector());
        // get b_i
        DataSet<Element> currentBeta = workset.filter(new FilterCurrentBeta());
        // do actual work
        // v_i+1 = A * v_i
        DataSet<Element> nextVector = Multiplication.multiply(corpus, currentVector, Config.idOfBasis).runOperation(new IncrementColumn());
        // scale nextVector
        nextVector = Multiplication.scalarDouble(nextVector, scaleFactorSet);
        // v_i+1 -= beta_i * v_i-1
        nextVector = Multiplication.substract(nextVector, Multiplication.scalar(previousVector, currentBeta));
        // alpha_i = v_i+1 . v_i
        DataSet<Element> currentAlpha = Multiplication.dot(currentVector, nextVector, Config.idOfTriag).map(new SetNewAlphaRowAndCol());
        // v_i+1 -= alpha_i * v_i
        nextVector = Multiplication.substract(nextVector, Multiplication.scalar(currentVector, currentAlpha));
        /*
            ### orthogonalization ###

            Exception in thread "main" org.apache.flink.compiler.CompilerException: Nested iterations are currently not supported.

            .....................................

         */

        DataSet<Element> initialOrthoWorkset = env.generateSequence(1, numRows).cross(env.generateSequence(1, desiredRank)).with((x,y) -> new Element(Config.idOfBasis, x, y, 0.0)).join(iteration.getSolutionSet()).where(0,1,2).equalTo(0,1,2).with((e1,e2)->e2);
        DataSet<Element> initialOrthoSolutionSet = nextVector;
        DeltaIteration<Element,Element> orthoIteration = initialOrthoSolutionSet.iterateDelta(initialOrthoWorkset, desiredRank, Element.ID, Element.ROW, Element.ROW);
        DataSet<Element> currentOrthoBaseVector = orthoIteration.getWorkset().runOperation(new FilterCurrentOrthoVector());
        DataSet<Element> pseudoAlpha = Multiplication.dot(nextVector, currentOrthoBaseVector, (byte) -1);
        nextVector = Multiplication.substract(nextVector, Multiplication.scalar(currentOrthoBaseVector, pseudoAlpha));
        DataSet<Element> nextOrthoWorkset = orthoIteration.getWorkset().filter(new RichFilterFunction<Element>() {
            @Override
            public boolean filter(Element e) throws Exception {
                int curStep = getIterationRuntimeContext().getSuperstepNumber();
                return e.getId() == Config.idOfBasis && e.getCol().compareTo(Long.valueOf(curStep)) > 0;
            }
        });
        nextVector = orthoIteration.closeWith(nextVector, nextOrthoWorkset);



//        DeltaIteration<Element,Element> orthoIteration = initialOrthoSolutionSet.iterateDelta(initialOrthoWorkset, desiredRank, Element.ID, Element.ROW, Element.ROW);
//        DataSet<Element> orthoWorkset = orthoIteration.getWorkset();
//        DataSet<Element> pseudoAlpha = Multiplication.dot(nextVector, workset, (byte) -1);
//        nextVector = Multiplication.substract(nextVector, Multiplication.scalar(orthoWorkset, pseudoAlpha));
//        DataSet<Element> nextOrthoWorkset = orthoIteration.getSolutionSet().filter(new RichFilterFunction<Element>() {
//            @Override
//            public boolean filter(Element e) throws Exception {
//                int curStep = getIterationRuntimeContext().getSuperstepNumber();
//                System.out.println(curStep);
//                return e.getId() == Config.idOfBasis && e.getCol().equals(curStep);
//            }
//        });
//        orthoIteration.closeWith(iteration.getSolutionSet(), nextOrthoWorkset);


        DataSet<Element> nextBeta = nextVector.runOperation(new CreateElementWithL2Norm(Config.idOfTriag)).map(new SetNewBetaRowAndCol());
        /*
            ### TBD range check ###
            this has to be some sort of nextWorkset = ...
            because emptying the nextWorkset is the only chance of bailing out of the iteration early
         */
        // b_i+1 = ||v_i+1||. So this normalizes v_i+1
        nextVector = Multiplication.scalarInverted(nextVector, nextBeta);
        /*
            ### prepare next iteration ###
         */

        // the next workset consists of v_i, v_i+1 and b_i+1
        DataSet<Element> nextWorkset = currentVector.union(nextVector.union(nextBeta));

        // in each iteration, we add v_i-1, b_i, and a_i
        DataSet<Element> solutionSetDelta = nextWorkset.union(previousVector.union(currentBeta.union(currentAlpha)));

        DataSet<Element> result = iteration.closeWith(solutionSetDelta, nextWorkset);

        // return the final result
        return finalizeLanczos(corpus, result, desiredRank, isSymmetric);

    }

    private static DataSet<Element> finalizeLanczos(DataSet<Element> corpus, DataSet<Element> result, int desiredRank, boolean isSymmetric) {
        // add last alpha
        DataSet<Element> lastVector = result.filter(new FilterBasisVector(Long.valueOf(desiredRank)));
        DataSet<Element> overflowVector = Multiplication.multiply(corpus, lastVector, Config.idOfBasis).runOperation(new IncrementColumn());
        DataSet<Element> lastAlpha = Multiplication.dot(lastVector, overflowVector, Config.idOfTriag).runOperation(new SetRowAndCol(Long.valueOf(desiredRank), Long.valueOf(desiredRank)));
        result = result.union(lastAlpha);
        // duplicate betas
        result = result.flatMap(new BetaDuplicator());
        return result;
    }


}
