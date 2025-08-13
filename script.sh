#!/bin/bash

# Directory containing the files
FOLDER="folder_a"

# Check if folder exists
if [ ! -d "$FOLDER" ]; then
    echo "Error: Directory $FOLDER does not exist"
    exit 1
fi

# Counter for new sequence numbers starting from 2000
counter=2000

# Change to the target directory
cd "$FOLDER"

# Find and rename files matching the pattern V20250812.*.sql
for file in V20250812.*.sql; do
    # Check if file exists (in case no files match the pattern)
    if [ ! -f "$file" ]; then
        echo "No files found matching pattern V20250812.*.sql"
        break
    fi
    
    # Extract the part after the first sequence number
    # Example: V20250812.0000.xxxx__xxx.sql -> xxxx__xxx.sql
    suffix=$(echo "$file" | sed 's/V20250812\.[0-9]\{4\}\.//')
    
    # Create new filename with updated date and sequence
    new_name="V20250813.$(printf "%04d" $counter).$suffix"
    
    # Rename the file
    mv "$file" "$new_name"
    echo "Renamed: $file -> $new_name"
    
    # Increment counter for next file
    ((counter++))
done

echo "Renaming complete!"
