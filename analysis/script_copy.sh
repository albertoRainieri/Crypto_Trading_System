#!/bin/bash

source_dir='/Users/albertorainieri/Projects/Personal/analysis/json'
destination_dir='/Volumes/ZX10/tracker_json'



# Check if the source directory exists
if [ ! -d "$source_dir" ]; then
    echo "Source directory does not exist: $source_dir"
    exit 1
fi

# Check if the destination directory exists; if not, create it
if [ ! -d "$destination_dir" ]; then
    mkdir -p "$destination_dir"
fi

# Use a for loop to iterate through files in the source directory
for file in "$source_dir"/*; do
    if [ -f "$file" ]; then
        # Copy each file to the destination directory
        cp "$file" "$destination_dir"/
        echo 'done'
    fi
done

# Display a success message
echo "Files copied from $source_dir to $destination_dir"