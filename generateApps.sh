#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd $DIR

#execute mvn plugin with app geneartion
mvn clean install scs:generate-app -pl :spring-cloud-task-app-generator
