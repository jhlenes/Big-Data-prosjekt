# TDT4305 - Big Data Architecture
The project of the course TDT4305 consists of two phases. The project work was done in the Spark-compatible language Scala.

## Phase 1: Data Analysis with Spark
The first phase focuses on learning how to perform data analysis on a large dataset with the Apache Spark framework. In this task we performed an exploratory analysis of a Twitter dataset.

## Phase 2: Data Analysis with Spark
The second phase focuses on creating a data analysis application for a large dataset with the Apache Spark framework. In this task we created a Spark application that will estimate the location for a tweet text in terms of place. This was done with a Naive Bayes classifier.

## Packaging the application
The `sbt package` command will create a runnable `.jar` and place it in `target/scala-2.11/bigdata_2.11-0.1.jar`.

## Running the program
First, make sure you are in the top directory (the folder this file is located in).

### Phase 1
```
%SPARK_HOME%/bin/spark-submit2.cmd --class "phase1.task_<task_num>" --master local[4] target/scala-2.11/bigdata_2.11-0.1.jar
```
where you replace `<task_num>` with the number of the task you want to run. `%SPARK_HOME%` is the location of your Spark installation.

#### Seeing the result
The result files will be created here: `/data/results/task_<task_num>.tsv`, where `<task_num>` is the number of the task you ran.

### Phase 2
```
%SPARK_HOME%/bin/spark-submit2.cmd --class "phase2.classify" --master local[4] target/scala-2.11/bigdata_2.11-0.1.jar -training <full path of the training file> -input <full path of the input file> -output <full path of the output file>
```
`%SPARK_HOME%` is the location of your Spark installation.
