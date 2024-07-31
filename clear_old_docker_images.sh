#!/bin/bash

# Get the current date in seconds since epoch
current_date=$(date +%s)

# Get the threshold date (7 days ago) in seconds since epoch
threshold_date=$(date -d '7 days ago' +%s)

# List all Docker images with their creation dates in JSON format
docker images --format '{{json .}}' | jq -r '. | select(.CreatedAt) | "\(.ID) \(.CreatedAt)"' > images_with_dates.txt

# Filter images older than a week
awk -v threshold_date="$threshold_date" '
{
    image_id = $1
    created_at = $2 " " $3 " " $4 " " $5 " " $6
    created_at_seconds = mktime(gensub(/-|:|T|Z/, " ", "g", created_at))
    if (created_at_seconds < threshold_date) {
        print image_id
    }
}' images_with_dates.txt > old_images.txt

# Remove the old images
if [ -s old_images.txt ]; then
    xargs -a old_images.txt docker rmi -f
else
    echo "No old images to remove."
fi

# Clean up
rm images_with_dates.txt old_images.txt
