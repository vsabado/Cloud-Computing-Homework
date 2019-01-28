#!/bin/bash

#Clear output
rm -R ./output   
#Compile
javac WordCount.java -Xlint:deprecation
#Build jar
jar -cvf WordCount.jar ./WordCount*.class
#Run hadoop
/usr/local/hadoop/bin/hadoop jar WordCount.jar WordCount input output
