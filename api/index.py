import json
import os
import sys
import tempfile
import shutil
from collections import defaultdict, Counter
from datetime import datetime
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

# Import Dulwich safely
try:
    from dulwich import porcelain
    from dulwich.repo import Repo
    from dulwich.diff import tree_changes
    from dulwich.objects import Tree
    DULWICH_AVAILABLE = True
    DULWICH_ERROR = None
except ImportError as e:
    DULWICH_AVAILABLE = False
    DULWICH_ERROR = str(e)

def get_commits(repo_path):
    """
    Retrieves commit history using Dulwich.
    """
    if not DULWICH_AVAILABLE: return []
    
    repo = Repo(repo_path)
    commits = []
    
    # Get walker (iterates from most recent backwards)
    try:
        walker = repo.get_walker()
    except Exception as e:
        print(f"Walker error: {e}")
        return []
    
    # Convert to list and reverse to get chronological order (oldest first)
    all_commits = list(walker)
    all_commits.reverse()
    
    empty_tree_id = Tree().id
    
    for commit in all_commits:
        files = []
        
        # Determine parent tree for diff
        if commit.parents:
            parent_tree_id = repo[commit.parents[0]].tree
        else:
            parent_tree_id = empty_tree_id
            
        current_tree_id = commit.tree
        
        # Get changes
        changes = tree_changes(repo.object_store, parent_tree_id, current_tree_id)
        
        for change in changes:
            # We want the filename. 
            # change.new is None for deletions, change.old is None for creations.
            # If both exist (modify), we use new path.
            fpath = None
            if change.new.path:
                fpath = change.new.path
            elif change.old.path:
                fpath = change.old.path
                
            if fpath:
                # Dulwich paths are bytes
                try:
                    files.append(fpath.decode('utf-8'))
                except:
                    files.append(str(fpath))

        # Parse author (Format: b"Name <email>")
        try:
            author = commit.author.decode('utf-8', errors='replace')
        except:
            author = str(commit.author)
            
        if '<' in author:
            author = author.split('<')[0].strip()

        # Parse subject
        try:
            subject = commit.message.decode('utf-8', errors='replace').split('\n')[0]
        except:
            subject = "No Subject"

        commits.append({
            "hash": commit.id.decode('utf-8'),
            "timestamp": commit.commit_time,
            "author": author,
            "subject": subject,
            "files": files
        })
            
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
        
        # Filter files - keep only code/text files roughly
        files = [f for f in files if '.' in f and not f.startswith('.git')]
        
        # Churn & Metadata
        for f in files:
            if f not in file_metadata:
                file_metadata[f] = {
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
                    # Sort pair to ensure (A, B) is same as (B, A)
                    pair = tuple(sorted((f1, f2)))
                    couplings[pair] += 1

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
    formatted_nodes = []
    for f, meta in file_metadata.items():
        formatted_nodes.append({
            "id": f,
            "group": get_file_type(f),
            "size": meta["size"],
            "owner": meta["owner"],
            "createdAt": meta["createdAt"]
        })

    links = []
    for pair, weight in couplings.items():
        source, target = pair
        if source in file_metadata and target in file_metadata:
            # Check weight usage (only include if > 1 to reduce noise? optional)
            if weight >= 1:
                link_time = max(file_metadata[source]["createdAt"], file_metadata[target]["createdAt"])
                links.append({
                    "source": source,
                    "target": target,
                    "weight": weight,
                    "createdAt": link_time
                })
            
    return {
        "nodes": formatted_nodes,
        "links": links
    }

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)
            repo_url = params.get('url', [None])[0]

            if not repo_url:
                # Health Check / Debug Info
                status = 200
                data = {"status": "ok", "dulwich_installed": DULWICH_AVAILABLE}
                if not DULWICH_AVAILABLE:
                    data["error"] = DULWICH_ERROR
                
                self.send_response(status)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(data).encode('utf-8'))
                return

            if not DULWICH_AVAILABLE:
                self.send_response(500)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"error": f"Dulwich (git) library missing: {DULWICH_ERROR}. Check server logs."}).encode('utf-8'))
                return

            temp_dir = None
            try:
                temp_dir = tempfile.mkdtemp()
                
                # Clone using Dulwich
                # supports https://...
                # Note: Dulwich clone is strictly Git protocol or HTTPS
                # print(f"Cloning {repo_url}...")
                porcelain.clone(repo_url, temp_dir)
                
                commits = get_commits(temp_dir)
                file_metadata, couplings = analyze_history(commits)
                data = generate_json(file_metadata, couplings)

                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(data).encode('utf-8'))

            except Exception as e:
                # Try to send error if headers not sent
                try:
                    self.send_response(500)
                except: pass
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
                 self.send_response(500)
                 self.send_header('Content-type', 'application/json')
                 self.end_headers()
                 self.wfile.write(json.dumps({"error": f"Server error: {str(outer_e)}"}).encode('utf-8'))
            except: pass
