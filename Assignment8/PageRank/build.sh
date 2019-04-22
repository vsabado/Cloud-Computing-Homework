#!/bin/bash

rm -rf targer project output
sbt package
/usr/local/spark/bin/spark-submit --class "pagerank" --master local[2] target/scala-2.12/pagerank_2.12-0.1.jar
