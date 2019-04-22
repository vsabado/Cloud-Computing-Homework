#!/bin/bash

rm -rf targer project output
sbt package
/usr/local/spark/bin/spark-submit --class "pagerank" --master local target/scala-2.11/simple-project_2.11-1.0.jar

