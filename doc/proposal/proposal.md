# AIM3 Project Proposal: Implementing Dimensionality Reduction in Apache Flink Using the Lanczos Algorithm

Fridtjof Sander (fsander@mailbox.tu-berlin.de), Nik Hille (nik.hille@campus.tu-berlin.de)

## Motivation and Problem Statement

Whenever high dimensionality of large datasets renders analytical processes unfeasable, dimensionality reduction is a useful approach to technically and semantically compress the data to its most important features. In Latent Semantic Indexing (LSI) for example this technique is exploited to extract abstract concepts from documents and to calculate "a document to concept"-mapping from "a document to containng words" mapping. The underlying technique of LSI to reduce dimensionality is Singular Value Decomposition (SVD), which can be achieved in a scalable way by exploiting certain properties of the Lanczos algorithm. The Lanczos algorithm works by repeatitly performing matrix-vector-multiplication.

The primary goal of our project is to implement the Lanczos algorithm in Apache Flink and using it to perform SVD on
arbitrary matrices. We are aiming to develop a tool/library that takes a matrix in some specific format as input and
delivers the result of performing SVD on that matrix. Our goal is not to analyze a specific data set. Instead we intend
to create a tool that we expect will greatly benefit from Flink's architecture.

## Project Plan

On a high level, the practical part of our project will consist of three phases: Obtaining the necessary prerequisites,
implementing the actual tool and evaluating the results.

### Obtaining the Prerequisites

Most importantly, we need a sample data set with high dimensionality that contains a known number of hidden concepts to compare our findings against. A large collection of news articels with known associated categories would be ideal, which is yet to be found, but a dump of the english Wikipedia should also work. Generating sample data by expanding a reduced set is also an option, but very expensive and thus should be avoided if possible. We also evaluate using Mahouts SVD implementation to compare the results.

### Implementation

In order to be able to implement the Lanczos algorithm, we will first need to develop a custom matrix 
representation/data model in Java/HDFS. Along with that, we are going to implement the mathematical matrix operations
used in the Lanczos algorithm. If our evaluation data is calculated by Mahout, we will
also evaluate whether or not it's feasible for us to focus on compatibility with the Mahout API. This could potentially
benefit future integration of our work into Flink itself and also improve comparability for a benchmark in the future.

### Evaluation

Our main experiment is using the tool we developed to perform dimensionality reduction on the sample data set, followed
by an evaluation of the result. If it is feasable to compare our findings against the known concept in our sample data set, we will do so. Otherwise we will
find a way to properly compare our result with the result of using Mahout's SVD functionality on the same data set. Of
course this also means that we will have to think about proper ways to rate the degree of similarity of the compared
matrices.
