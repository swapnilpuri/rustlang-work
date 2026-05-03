#!/bin/bash

# Script to remove GitHub remote from all project folders

WORKSPACE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Starting to remove git remotes from all projects in: $WORKSPACE_DIR"
echo "==========================================================="

# List of project directories
projects=(
  "loadDBForRust"
  "myproject"
  "oracle_query_app"
  "rustexamples"
  "var2project"
  "varborrowing"
  "varcloneproject"
  "varproject"
)

# Iterate through each project
for project in "${projects[@]}"; do
  project_path="$WORKSPACE_DIR/$project"
  
  if [ -d "$project_path/.git" ]; then
    echo ""
    echo "Processing: $project"
    cd "$project_path"
    
    # Show current remotes before
    echo "  Before: $(git remote -v)"
    
    # Remove the origin remote
    git remote remove origin
    
    # Show remotes after
    echo "  After:  $(git remote -v)"
    echo "  ✓ Remote removed from $project"
  else
    echo ""
    echo "⚠ Skipping: $project (not a git repository)"
  fi
done

echo ""
echo "==========================================================="
echo "✓ All projects processed!"
