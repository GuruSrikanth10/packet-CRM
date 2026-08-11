#!/usr/bin/env python3
import sqlite3
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.utils.paths import CHECKPOINT_DB_PATH, REPO_ROOT

def main():
    parser = argparse.ArgumentParser(description="Prune LangGraph checkpoints for completed packets.")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be deleted without actually deleting")
    args = parser.parse_args()

    base_dir = REPO_ROOT
    db_path = CHECKPOINT_DB_PATH

    if not db_path.exists():
        print(f"Database not found at {db_path}")
        return
        
    casebook_dir = base_dir / "local_casesheets"
    
    print(f"Connecting to SQLite database: {db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 1. Find all threads in the checkpoints database
    cursor.execute("SELECT DISTINCT thread_id FROM checkpoints")
    threads = [row[0] for row in cursor.fetchall()]
    
    print(f"Found {len(threads)} total threads in checkpoints.db")
    
    to_delete = []
    
    # 2. Check which threads have a terminal casebook
    for thread_id in threads:
        casebook_file = casebook_dir / f"casebook_{thread_id}" / "casebook.json"
        if casebook_file.exists():
            # If the final casebook exists, the pipeline completed successfully
            # (or reached a terminal error state)
            to_delete.append(thread_id)
            
    print(f"Found {len(to_delete)} threads eligible for pruning (terminal state reached).")
    
    if not to_delete:
        print("Nothing to prune.")
        return
        
    if args.dry_run:
        print("\nDRY RUN - Would delete checkpoints for the following threads:")
        for t in to_delete:
            print(f"  - {t}")
        return
        
    # 3. Delete the checkpoints
    print("\nExecuting deletions...")
    deleted_checkpoints = 0
    deleted_writes = 0
    deleted_blobs = 0
    
    for thread_id in to_delete:
        # LangGraph SqliteSaver uses tables: checkpoints, checkpoints_writes, (and blobs if using newer versions, but we'll try to catch standard tables)
        # We delete by thread_id
        
        # We need to make sure we don't fail if a table doesn't exist, so we try each
        tables = ["checkpoints", "checkpoints_writes", "checkpoints_blobs", "checkpoints_writes_blobs", "checkpoints_blobs_writes"]
        
        for table in tables:
            try:
                cursor.execute(f"DELETE FROM {table} WHERE thread_id = ?", (thread_id,))
                if table == "checkpoints":
                    deleted_checkpoints += cursor.rowcount
                elif table == "checkpoints_writes":
                    deleted_writes += cursor.rowcount
                elif "blobs" in table:
                    deleted_blobs += cursor.rowcount
            except sqlite3.OperationalError:
                # Table might not exist, that's fine
                pass
                
    conn.commit()
    conn.execute("VACUUM") # Reclaim disk space
    conn.close()
    
    print("\n✅ Pruning Complete!")
    print(f"Deleted {deleted_checkpoints} rows from checkpoints")
    print(f"Deleted {deleted_writes} rows from checkpoints_writes")
    print(f"Deleted {deleted_blobs} rows from checkpoints_blobs (if applicable)")

if __name__ == "__main__":
    main()
