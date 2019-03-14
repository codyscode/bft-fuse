#!/bin/bash


if [ $1 = "server" ]; then
	mkdir -p replicas/replica"$2"
	java -cp build/classes/main:build/libs/* BFTServer "$2" ./replicas/replica"$2"
	if [ $? = 1 ]; then
		echo "Build the project first with: ./demo.sh build"
	fi

elif [ $1 = "client" ]; then
	mkdir -p clients/client"$2"
	java -cp build/classes/main:build/libs/* BFTFuse "$2" ./clients/client"$2"
	if [ $? = 1 ]; then
		echo "Build the project first with: ./demo.sh build"
		echo "Make sure servers say "Ready to process operations" before connecting client"
	fi

elif [ $1 = "build" ]; then
	gradle build
	gradle libs

elif [ $1 = "clean" ]; then
	gradle clean
	rm -rf clients
	echo
	read -p "Do you want to delete the files/folders you made during the demo? (y/n): " choice
	if [ $choice = "y" ]; then
		rm -rf replicas
	fi
else 
	echo "Invalid option"

fi


