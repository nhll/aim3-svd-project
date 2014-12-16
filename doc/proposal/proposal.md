# AIM3 Project Proposal: Implementing Dimensionality Reduction in Apache Flink Using the Lanczos Algorithm

Fridtjof Sander (fsander@mailbox.tu-berlin.de), Nik Hille (nik.hille@campus.tu-berlin.de)

## Motivation and Problem Statement

For many Big Data applications, the concept of dimensionality reduction is a very useful approach to deal with data
represented as tremendously large matrices. It can be achieved through singular value decomposition (SVD), which is a 
core technique in latent semantic indexing (LSI). LSI is often used for searching large amounts of data, such as the
internet, as it has the ability to extract the conceptual content of a body of text. One way to achieve dimensionality
reduction is the Lanczos algorithm, which mainly consists of matrix multiplication and is thus highly 'parallelizable'.

The primary goal of our project is implementing the Lanczos algorithm in Apache Flink and using it to perform SVD on
arbitrary matrices. We are aiming to develop a tool/library that takes a matrix in some specific format as input and
delivers the result of performing SVD on that matrix. Our goal is not to analyze a specific data set. Instead we intend
to create a tool that we hope will greatly benefit from Flink's architecture.

## Project Plan

On a high level, the practical part of our project will consist of three phases: Obtaining the necessary prerequisites,
implementing the actual tool and evaluating the results.

### Obtaining the Prerequisites

First, we are going to search for a sample data set that satisfies our requirements. This will most likely be a
sufficiently large term-document-matrix of some sort. The best case would be to find a data set for which an SVD 
solution already exists, as this would make the evaluation of our implementation relatively easy. If we can't find such
a data set, we plan on using the Mahout machine learning library for generating a sample data set instead.

### Implementation

In order to be able to implement the Lanczos algorithm, we will first need to develop a custom matrix 
representation/data model in Java/HDFS. Along with that, we are going to implement the mathematical matrix operations
used in the Lanczos algorithm. If our sample data set has to be generated through Mahout (see previous section), we will
also evaluate whether or not it's feasible for us to focus on compatibility with the Mahout API. This could potentially
benefit future integration of our work into Flink itself.

### Evaluation

Our main experiment is using the tool we developed to perform dimensionality reduction on the sample data set, followed
by an evaluation of the result. If we are able to use a data set, for which an SVD solution already exists (see section
'Obtaining the Prerequisites'), evaluating our result means comparing it to the reference solution. Otherwise we will
find a way to properly compare our result with the result of using Mahout's SVD functionality on the same data set. Of
course this also means that we will have to think about proper ways to rate the degree of similarity of the compared
matrices.
