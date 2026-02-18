import subprocess
import json
import os
import sys
import argparse
import tempfile
import shutil
from collections import defaultdict, Counter
from datetime import datetime

def run_git_log(repo_path):
    """
    Executes git log to retrieve commit history in the specified repo path.
    """
    cmd = [
        "git",
        "log",
        "--reverse",
        "--pretty=format:%H|%at|%aN|%s",
        "--name-only",
        "--no-merges"
    ]
    
    try:
        # Run git command
        result = subprocess.run(cmd, cwd=repo_path, capture_output=True, text=True, encoding='utf-8', errors='replace')
        if result.returncode != 0:
            print(f"Error executing git log: {result.stderr}")
            sys.exit(1)
        return result.stdout
    except Exception as e:
        print(f"Failed to run git log: {e}")
        sys.exit(1)

def clone_repo(url, temp_dir):
    """
    Clones the repo metadata only (partial clone) to temp_dir.
    """
    print(f"Cloning {url} (metadata only)...")
    cmd = [
        "git",
        "clone",
        "--filter=blob:none", # Don't download file contents
        "--no-checkout",      # Don't check out files to disk
        url,
        temp_dir
    ]
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Failed to clone repo: {e}")
        sys.exit(1)

def parse_log(log_output):
    """
    Parses the git log output into a structured format.
    """
    commits = []
    current_commit = None
    
    lines = log_output.strip().split('\n')
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        if '|' in line and len(line.split('|')) >= 3:
            # New commit header
            parts = line.split('|')
            commit_hash = parts[0]
            timestamp = int(parts[1])
            author = parts[2]
            subject = parts[3] if len(parts) > 3 else ""
            
            current_commit = {
                "hash": commit_hash,
                "timestamp": timestamp,
                "author": author,
                "subject": subject,
                "files": []
            }
            commits.append(current_commit)
        else:
            # File path
            if current_commit:
                current_commit["files"].append(line)
                
    return commits

def analyze_history(commits):
    """
    Calculates churn and coupling.
    """
    file_metadata = {} # path -> {createdAt, size(churn), owner}
    couplings = Counter() # (fileA, fileB) -> count
    
    for commit in commits:
        files = commit["files"]
        timestamp = commit["timestamp"]
        author = commit["author"]
        
        # Sort files to ensure consistent key for couplings
        files.sort()
        
        # Churn and Metadata
        for f in files:
            if f not in file_metadata:
                file_metadata[f] = {
                    "id": f,
                    "label": os.path.basename(f),
                    "type": get_file_type(f),
                    "size": 1, # Initial size/churn
                    "createdAt": timestamp,
                    "owner": author
                }
            else:
                file_metadata[f]["size"] += 1
        
        # Coupling
        if len(files) > 1:
            for i in range(len(files)):
                for j in range(i + 1, len(files)):
                    f1 = files[i]
                    f2 = files[j]
                    couplings[(f1, f2)] += 1

    return file_metadata, couplings

def get_file_type(filepath):
    """
    Determines file type based on extension.
    """
    ext = os.path.splitext(filepath)[1].lower()
    mapping = {
        '.py': 'PYTHON',
        '.js': 'JS',
        '.html': 'HTML',
        '.css': 'CSS',
        '.json': 'JSON',
        '.md': 'DOCS',
        '.txt': 'TEXT',
        '.c': 'C',
        '.cpp': 'CPP',
        '.h': 'HEADER',
        '.java': 'JAVA',
        '.go': 'GO',
        '.rs': 'RUST',
        '.ts': 'TS',
        '.jsx': 'REACT',
        '.tsx': 'REACT'
    }
    return mapping.get(ext, 'OTHER')

def generate_json(file_metadata, couplings):
    """
    Generates the final JSON structure.
    """
    nodes = list(file_metadata.values())
    links = []
    
    for pair, weight in couplings.items():
        source, target = pair
        if source in file_metadata and target in file_metadata:
            link_time = max(file_metadata[source]["createdAt"], file_metadata[target]["createdAt"])
            links.append({
                "source": source,
                "target": target,
                "weight": weight,
                "createdAt": link_time
            })
            
    return {
        "nodes": nodes,
        "links": links
    }

def main():
    parser = argparse.ArgumentParser(description="Visualize Git Evolution")
    parser.add_argument("repo_url", nargs="?", help="Optional URL of a remote git repository to analyze")
    args = parser.parse_args()

    repo_path = os.getcwd()
    temp_dir = None

    if args.repo_url:
        temp_dir = tempfile.mkdtemp()
        clone_repo(args.repo_url, temp_dir)
        repo_path = temp_dir
    
    try:
        print(f"Fetching git history from {repo_path}...")
        log_output = run_git_log(repo_path)
        
        print(f"Parsing {len(log_output.splitlines())} lines of log...")
        commits = parse_log(log_output)
        print(f"Found {len(commits)} commits.")
        
        print("Analyzing evolution...")
        file_metadata, couplings = analyze_history(commits)
        print(f"Tracked {len(file_metadata)} files and {len(couplings)} coupled pairs.")
        
        print("Generating JSON...")
        data = generate_json(file_metadata, couplings)
        
        output_file = "evolution.json"
        with open(output_file, "w") as f:
            json.dump(data, f, indent=2)
            
        print(f"Done! Saved to {output_file}")
        
    finally:
        if temp_dir:
            print("Cleaning up temporary directory...")
            # simplified cleanup
            def on_rm_error(func, path, exc_info):
                os.chmod(path, 0o777)
                try:
                    func(path)
                except Exception:
                    pass
            shutil.rmtree(temp_dir, onerror=on_rm_error)

if __name__ == "__main__":
    main()
