#!/bin/bash
if [ -z $1 ];
then
    echo "Usage: build.sh <docker repo> <version>"
    echo "This builds and pushes an updated image to a docker repo"
    exit 1
fi

docker login
docker build . -t $1/weather-api:$2
docker push $1/weather-api:$2