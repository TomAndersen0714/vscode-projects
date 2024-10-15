#!/bin/bash

# set -ex


a='2023-12-07'
b="2023-12-47"

echo ${a:0:4} ${b:0:4}


mvn archetype:generate -DgroupId=com.example.java -DartifactId=my-second-java-project -DarchetypeArtifactId=maven-archetype-quickstart -DarchetypeVersion=1.5 -DinteractiveMode=false