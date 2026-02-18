import subprocess
import json
import os
import sys
import argparse
import tempfile
import shutil
from collections import defaultdict, Counter
from datetime import datetime
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

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

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            # CORS Headers
            try:
                self.send_response(200)
            except: pass
            
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)
            repo_url = params.get('url', [None])[0]

            if not repo_url:
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Missing 'url' parameter"}).encode('utf-8'))
                return

            # Check for git
            if shutil.which("git") is None:
               self.send_header('Content-type', 'application/json')
               self.end_headers()
               self.wfile.write(json.dumps({"error": "Git not installed on server environment"}).encode('utf-8'))
               return

            temp_dir = None
            try:
                temp_dir = tempfile.mkdtemp()
                clone_repo(repo_url, temp_dir)
                log_output = run_git_log(temp_dir)
                commits = parse_log(log_output)
                file_metadata, couplings = analyze_history(commits)
                data = generate_json(file_metadata, couplings)

                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(data).encode('utf-8'))

            except Exception as e:
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"error": f"Analysis failed: {str(e)}"}).encode('utf-8'))
            
            finally:
                if temp_dir and os.path.exists(temp_dir):
                    def on_rm_error(func, path, exc_info):
                        try:
                            os.chmod(path, 0o777)
                            func(path)
                        except: pass
                    shutil.rmtree(temp_dir, onerror=on_rm_error)
        except Exception as outer_e:
            try:
                 self.send_header('Content-type', 'application/json')
                 self.end_headers()
                 self.wfile.write(json.dumps({"error": f"Server error: {str(outer_e)}"}).encode('utf-8'))
            except: pass

if __name__ == "__main__":
    main()
