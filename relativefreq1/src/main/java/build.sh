#!/bin/bash

#Clear output
rm -R ./output   
#Compile
javac *.java -Xlint:deprecation -Xlint:unchecked
#Build jar
jar -cvf relativefreq2.jar ./*.class
#Run hadoop
/usr/local/hadoop/bin/hadoop jar relativefreq2.jar relativefreq2 input output
