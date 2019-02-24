#!/bin/bash

#Clear output
rm -R ./output   
#Compile
javac *.java -Xlint:deprecation -Xlint:unchecked
#Build jar
jar -cvf relativefreq1.jar ./*.class
#Run hadoop
/usr/local/hadoop/bin/hadoop jar relativefreq1.jar relativefreq1 input output
