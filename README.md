# TDT4305 Big Data Project

## Phase 1: Data Analysis with Spark
`TODO: Description of the task`

## Running the program
First, make sure you are in the top directory (the folder this file is located in). If you clone directly from Github,
this will be the `Big-Data-prosjekt/` folder.

### Packaging the application
The `sbt package` command will create a runnable `.jar` and place it in `target/scala-2.11/bigdata_2.11-0.1.jar`.

### Running the tasks
To run the tasks, use the following command:
```
%SPARK_HOME%/bin/spark-submit2.cmd --class "task_<task_num>" --master local[4] target/scala-2.11/bigdata_2.11-0.1.jar
```
where you replace `<task_num>` with the number of the task you want to run. `%SPARK_HOME%` is the location of your Spark installation.

### Seeing the result
The result files will be created here: `Big-Data-prosjekt/task_<task_num>.tsv`, where `<task_num>` is the number of the task you ran.
