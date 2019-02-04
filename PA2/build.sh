#!/bin/bash

#Clear output
rm -R ./output   
#Compile
javac BigramCount.java -Xlint:deprecation
#Build jar
jar -cvf BigramCount.jar ./BigramCount*.class
#Run hadoop
/usr/local/hadoop/bin/hadoop jar BigramCount.jar BigramCount input output
