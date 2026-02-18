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
    from dulwich.diff_tree import tree_changes
    from dulwich.objects import Tree
    DULWICH_AVAILABLE = True
    DULWICH_ERROR = None
except ImportError:
    # Try alternate import location just in case
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
    
    for entry in all_commits:
        # dulwich.walker.Walker returns WalkEntry objects
        # We need to get the actual commit object
        try:
            commit = entry.commit
        except AttributeError:
            # Fallback if it's already a commit object (older versions?)
            commit = entry

        files = []
        
        # Determine parent tree for diff
        if commit.parents:
            parent_tree_id = repo[commit.parents[0]].tree
        else:
            # Initial commit: diff against empty tree.
            # However, in partial clones, the empty tree object might not exist.
            # We can use None to indicate "new root".
             parent_tree_id = None
            
        current_tree_id = commit.tree
        
        # Get changes
        if parent_tree_id is None:
             # If no parent, we simply walk the current tree recursively
             # But tree_changes requires two trees.
             # Alternative: use repo.object_store.tree_changes with empty_tree_id ONLY if it exists?
             # Or construct a virtual empty tree?
             # Actually diff_tree.tree_changes handles None as "empty" usually, let's verify.
             # Inspecting dulwich source, tree_changes(store, tree1, tree2)
             # If tree1 is None, it should treat as empty.
             pass
             
        # tree_changes(store, tree1_id, tree2_id)
        # If we pass None for tree1_id, does dulwich handle it?
        # Based on error, it tried to lookup empty_tree_id and failed.
        # Let's see if we can pass None.
        
        changes = tree_changes(repo.object_store, parent_tree_id, current_tree_id)
        
        for change in changes:
            # We want the filename. 
            # change.new is None for deletions, change.old is None for creations.
            # If both exist (modify), we use new path.
            fpath = None
            if change.new and change.new.path:
                fpath = change.new.path
            elif change.old and change.old.path:
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

def analyze_history(commits, subpath=""):
    """
    Calculates churn and coupling.
    Supports subpath filtering and relative clustering.
    """
    file_metadata = {} # path -> {createdAt, size(churn), owner}
    couplings = Counter() # (fileA, fileB) -> count
    
    # First pass: identify all files to check count
    all_files = set()
    for commit in commits:
        files = [f for f in commit["files"] if not f.startswith('.git')]
        if subpath:
            # Filter by subpath (must start with subpath/)
            # We add a trailing slash to subpath to ensure we match directories
            prefix = subpath if subpath.endswith('/') else subpath + '/'
            files = [f for f in files if f.startswith(prefix) or f == subpath]
        all_files.update(files)
        
    # Smart Aggregation Logic
    # If we have too many nodes (> 150) in this view, we cluster them by directory
    USE_CLUSTERING = len(all_files) > 150
    
    for commit in commits:
        files = commit["files"]
        timestamp = commit["timestamp"]
        author = commit["author"]
        
        # Filter files - relaxed
        files = [f for f in files if not f.startswith('.git/') and not f == '.git']
        
        if subpath:
             prefix = subpath if subpath.endswith('/') else subpath + '/'
             files = [f for f in files if f.startswith(prefix) or f == subpath]
        
        if not files:
            continue
        
        if USE_CLUSTERING:
            # Map files to their parent directories relative to subpath
            clustered_files = set()
            for f in files:
                # Get path relative to current drill-down level
                # e.g. subpath="src", f="src/ui/Button.tsx" -> rel="ui/Button.tsx"
                if f == subpath:
                    clustered_files.add(f)
                    continue
                    
                rel_path = f[len(subpath):]
                if rel_path.startswith('/'): rel_path = rel_path[1:]
                
                if '/' in rel_path:
                    # It has a subdirectory, group by that
                    top_dir = rel_path.split('/')[0]
                    # Reconstruct full path for uniqueness
                    full_cluster_path = f"{subpath}/{top_dir}" if subpath else top_dir
                    clustered_files.add(full_cluster_path)
                else:
                    # It's a direct child file, keep it
                    clustered_files.add(f)
            
            # Use the unique set of directories for this commit
            files = list(clustered_files)

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
    # If no extension, assume it's a folder or special file
    if not ext:
        # Heuristic: Uppercase usually Makefile, LICENSE, etc.
        # Lowercase usually folder
        if os.path.basename(filepath).isupper():
            return 'CONFIG'
        return 'FOLDER'
        
    return mapping.get(ext, 'OTHER')

def generate_json(file_metadata, couplings):
    """
    Generates the final JSON structure.
    """
    formatted_nodes = []
    for f, meta in file_metadata.items():
        # Frontend expects 'label' and 'type'
        # Group is used for color/grouping, type is used for display
        group = get_file_type(f)
        formatted_nodes.append({
            "id": f,
            "label": f, # Use filename as label
            "group": group,
            "type": group,
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
            subpath = params.get('subpath', [""])[0] 
            if subpath and subpath.endswith('/'): subpath = subpath[:-1]

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
                # usage: porcelain.clone(source, target, bare=False, checkout=False, depth=None)
                # We use depth=500 to avoid disk space issues on Vercel with large repos
                # print(f"Cloning {repo_url} with depth 500...")
                porcelain.clone(repo_url, temp_dir, depth=500)
                
                commits = get_commits(temp_dir)
                repo_url = params.get('url', [None])[0]
                subpath = params.get('subpath', [""])[0]
                # Clean subpath
                if subpath and subpath.endswith('/'): subpath = subpath[:-1]

                # ... (rest of code)
                
                # Clone using Dulwich
                # supports https://...
                # usage: porcelain.clone(source, target, bare=False, checkout=False, depth=None)
                # We use depth=500 to avoid disk space issues on Vercel with large repos
                # print(f"Cloning {repo_url} with depth 500...")
                porcelain.clone(repo_url, temp_dir, depth=500)
                
                commits = get_commits(temp_dir)
                file_metadata, couplings = analyze_history(commits, subpath)
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
