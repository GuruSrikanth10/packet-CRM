import os
import json
import subprocess
from filelock import FileLock

def promote_rules():
    base_dir = os.path.dirname(os.path.dirname(__file__))
    pending_file = os.path.join(base_dir, "prompts", "pending_rules.jsonl")
    lock_file = pending_file + ".lock"
    target_file = os.path.join(base_dir, "prompts", "InvestigatorAgent.md")
    
    if not os.path.exists(pending_file):
        print("No pending rules to promote.")
        return
        
    with FileLock(lock_file, timeout=10):
        with open(pending_file, "r", encoding="utf-8") as f:
            lines = f.readlines()
            
        if not lines:
            print("No pending rules to promote.")
            return
            
        print(f"Found {len(lines)} pending rules.")
        
        # We process them sequentially
        promoted_count = 0
        for i, line in enumerate(lines):
            try:
                entry = json.loads(line.strip())
                print(f"\n--- Rule {i+1} ---")
                print(f"Event ID: {entry.get('eventId')}")
                print(f"Reasoning: {entry.get('reviewer_reasoning')}")
                print(f"Proposed Rule: {entry.get('proposed_rule')}")
                
                choice = input("\nPromote this rule? (y/N): ").strip().lower()
                if choice == 'y':
                    # Append to InvestigatorAgent.md
                    with open(target_file, "a", encoding="utf-8") as f_target:
                        f_target.write(f"\n- CRITICAL RULE: {entry.get('proposed_rule')}\n")
                        
                    # Commit via git
                    commit_msg = f"Add learning rule from event {entry.get('eventId')}"
                    subprocess.run(["git", "add", target_file], check=True)
                    subprocess.run(["git", "commit", "-m", commit_msg], check=True)
                    print("Rule promoted and committed!")
                    promoted_count += 1
                else:
                    print("Rule skipped.")
            except Exception as e:
                print(f"Error processing rule {i+1}: {e}")
                
        # Clear the pending file after processing
        with open(pending_file, "w", encoding="utf-8") as f:
            pass
            
        print(f"\nFinished. Promoted {promoted_count} rules.")

if __name__ == "__main__":
    promote_rules()
