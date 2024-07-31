#!/bin/bash

# Variables
IMAGE_NAME="my-image"
FILE_PATH_INSIDE_CONTAINER="/var/prod_requirements.txt"
DESTINATION_PATH_ON_HOST="./prod_requirements.txt"

# Build the Docker image
# docker build -t $IMAGE_NAME .

# Create a container from the image without running it
CONTAINER_ID=$(docker create $IMAGE_NAME)

# Copy the file from the container to the host
docker cp $CONTAINER_ID:$FILE_PATH_INSIDE_CONTAINER $DESTINATION_PATH_ON_HOST

# Remove the container
docker rm -f $CONTAINER_ID

echo "File has been copied to $DESTINATION_PATH_ON_HOST"
