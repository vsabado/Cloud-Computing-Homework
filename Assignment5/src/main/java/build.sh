#!/bin/bash

#Clear output
rm -R ./output   
#Compile
javac *.java -Xlint:deprecation -Xlint:unchecked
#Build jar
jar -cvf invertedindex.jar ./*.class
#Run hadoop
/usr/local/hadoop/bin/hadoop jar invertedindex.jar invertedindex input output
