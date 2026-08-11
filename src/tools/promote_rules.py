import os
import json
import subprocess
from filelock import FileLock

def promote_rules():
    base_dir = os.path.dirname(os.path.dirname(__file__))
    prompts_dir = os.path.join(base_dir, "prompts")
    pending_file = os.path.join(prompts_dir, "pending_rules.jsonl")
    file_lock_path = pending_file + ".lock"
    promo_lock_path = os.path.join(prompts_dir, "promotion.lock")
    target_file = os.path.join(prompts_dir, "InvestigatorAgent.md")
    
    # 1. Top-level lock to prevent concurrent promotions by multiple humans/scripts
    promo_lock = FileLock(promo_lock_path, timeout=0)
    try:
        promo_lock.acquire()
    except Exception:
        print("Another promotion process is currently running. Exiting.")
        return
        
    try:
        # 2. Git status check
        result = subprocess.run(["git", "status", "--porcelain", prompts_dir], capture_output=True, text=True)
        if result.stdout.strip():
            print(f"Refusing to promote: uncommitted changes exist in {prompts_dir}")
            print(result.stdout)
            return

        if not os.path.exists(pending_file):
            print("No pending rules to promote.")
            return
            
        # 3. Read pending rules holding the file lock briefly
        with FileLock(file_lock_path, timeout=10):
            with open(pending_file, "r", encoding="utf-8") as f:
                lines = f.readlines()
                
        if not lines:
            print("No pending rules to promote.")
            return
            
        print(f"Found {len(lines)} pending rules.")

        # We process them sequentially. Only lines that are actually
        # promoted get removed below -- skipped rules, rules that errored,
        # and anything a running agent appends to pending_file during this
        # (potentially long, interactive) loop must all survive (1.7).
        promoted_count = 0
        promoted_raw_lines = set()
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
                    promoted_raw_lines.add(line.strip())
                else:
                    print("Rule skipped.")
            except Exception as e:
                print(f"Error processing rule {i+1}: {e}")

        # Rewrite the pending file, keeping every entry that was not
        # promoted. Re-read fresh (rather than reusing the stale `lines`
        # from the initial read) so anything appended concurrently during
        # this interactive session is preserved too.
        with FileLock(file_lock_path, timeout=10):
            with open(pending_file, "r", encoding="utf-8") as f:
                current_lines = f.readlines()
            remaining_lines = [ln for ln in current_lines if ln.strip() not in promoted_raw_lines]
            with open(pending_file, "w", encoding="utf-8") as f:
                f.writelines(remaining_lines)

        print(f"\nFinished. Promoted {promoted_count} rules.")
    finally:
        promo_lock.release()

if __name__ == "__main__":
    promote_rules()
