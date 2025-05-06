#!/bin/bash

# Make sure the script stops on errors
set -e

# Create a docs directory if it doesn't exist
mkdir -p docs

# Run pdoc to generate the documentation
# We use --exclude to ignore test directories
# Output is saved to the docs directory
pdoc -o docs \
    main.py \
    gatherer

echo "Documentation generated successfully in docs directory"