#!/bin/bash

#Clear output
rm -R output-graph*   
#Compile
javac *.java
#Build jar
jar -cvf GraphSearch.jar ./*.class
#Run hadoop
/usr/local/hadoop/bin/hadoop jar GraphSearch.jar GraphSearch -m 3 -r 3 -i 10
