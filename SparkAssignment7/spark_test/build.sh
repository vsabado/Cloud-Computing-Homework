#!/bin/bash

rm -rf targer project output
sbt package
/usr/local/spark/bin/spark-submit --class "WordCount" --master local[4] target/scala-2.12/simple-project_2.11-1.0.jar

