#!/bin/bash

mkdir replicas
mkdir replicas/replica0
mkdir replicas/replica1
mkdir replicas/replica2
mkdir replicas/replica3
mkdir mntp

gradle build
gradle libs

gnome-terminal --tab --title="Replica0" --command="bash -c 'java -cp build/classes/main:build/libs/* BFTServer 0 ./replicas/replica0; $SHELL'"
gnome-terminal --tab --title="Replica1" --command="bash -c 'java -cp build/classes/main:build/libs/* BFTServer 1 ./replicas/replica1; $SHELL'"
gnome-terminal --tab --title="Replica2" --command="bash -c 'java -cp build/classes/main:build/libs/* BFTServer 2 ./replicas/replica2; $SHELL'"
gnome-terminal --tab --title="Replica3" --command="bash -c 'java -cp build/classes/main:build/libs/* BFTServer 3 ./replicas/replica3; $SHELL'"

echo 
read -n 1 -p "Press any key once replicas are ready. They will say \"Ready to process operations\" or \"Server socket timed out\""

java -cp build/classes/main:build/libs/* BFTClient 0 ./mntp




