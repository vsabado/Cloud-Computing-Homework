#!/bin/bash

#Clear output
rm -R ./output   
#Compile
javac *.java -Xlint:deprecation
#Build jar
jar -cvf WordCount.jar ./*.class
#Run hadoop
/usr/local/hadoop/bin/hadoop jar WordCount.jar WordCount input output
