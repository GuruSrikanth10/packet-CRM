# Packet-CRM: Deep Dive and Architecture

## Overview
**Packet-CRM** is an AI-driven, self-learning service built to ingest, analyze, and resolve rejected biometric packets within the UIDAI ecosystem. 

When a packet fails enrollment or deduplication (e.g., due to a `RESIDENT_MAN_DEDUP_REJECT_TD` error), the system automatically spins up a suite of LangGraph-powered LLM agents. These agents investigate the error against a rules database, validate their findings, permanently learn from their mistakes, and format the output into structured JSON casebooks.

---

## 1. High-Level Workflow

```mermaid
sequenceDiagram
    participant K as Kafka Queue
    participant C as Background Consumer
    participant API as FastAPI (/process-rejection)
    participant M as RejectionManagerAgent
    participant I as InvestigatorAgent
    participant R as ReviewerAgent
    participant S as SynthesisAgent
    participant T as Tool Registry
    participant FS as Local Filesystem

    K->>C: Push Rejected Packet JSON
    C->>C: Filter packetStatus == "REJECTED"
    C->>API: HTTP POST /process-rejection
    
    API->>API: Validate via Pydantic (MessagePayload)
    API->>M: Invoke Orchestrator
    
    M->>I: Dispatch for Investigation
    I->>T: lookup_rule_by_reason_code
    I->>T: lookup_resident_database
    I-->>M: Return detailed technical findings
    
    M->>R: Dispatch findings for Validation
    R->>R: Validate logic and accuracy
    
    alt Mistake Detected
        R->>T: add_learning_rule()
        T->>FS: Appends constraint to InvestigatorAgent.md
        R-->>M: Return corrected findings
    else Validated
        R-->>M: Confirm findings
    end
    
    M->>S: Dispatch for Synthesis
    S->>S: Formulate Resolution & Action Enums
    S-->>M: Return strict analytical JSON
    
    M-->>API: Pass analytical JSON to backend
    API->>API: Extract metadata & construct hierarchical Casebook
    API->>FS: Save to local_casesheets/casebook_{eventId}/casebook.json
```

---

## 2. Directory Structure

The repository follows standard Python backend architecture for modularity and scalability:

```text
packet-CRM/
├── .agents/                        
│   └── AGENTS.md                   # Agentic configurations and behavioral rules
├── src/
│   ├── main.py                     # App entry point, daemon lifecycle manager
│   ├── api/
│   │   └── routes.py               # REST endpoints (/process-rejection)
│   ├── core/
│   │   └── agent_orchestrator.py   # LangGraph initialization and LLM provisioning
│   ├── models/
│   │   └── schemas.py              # Strict Pydantic data validation schemas
│   ├── prompts/                    
│   │   ├── manager.md              # RejectionManager orchestration logic
│   │   ├── InvestigatorAgent.md    # Investigator context and instructions
│   │   └── ReviewerAgent.md        # Reviewer context and validation logic
│   ├── config/                     
│   │   └── agents.json             # Map of Subagents to their prompts and tools
│   ├── tools/
│   │   └── tool_registry.py        # Custom Python tools (DB lookup, self-learning)
│   └── utils/
│       ├── env.py                  # Environment variable configuration
│       ├── kafkaConsumer.py        # Background topic polling
│       ├── llm_utils.py            # Local LLM and HF model factory
│       └── s3_client.py            # (Disabled) Cloud storage integrations
```

---

## 3. Core Components Deep Dive

### 3.1 Environment Configuration (`.env`)
The system manages all operational feature flags, LLM credentials, MySQL database connections, and Kafka connectivity settings via a strictly typed `.env` file (loaded via `python-dotenv` in `src/utils/env.py`).
- **Template:** A reference file containing all placeholders is available at `.env.example`.
- **Database Modes:** Set `USE_MOCK_DB=true` to parse rules locally from a CSV, or `USE_MOCK_DB=false` to dynamically query the live MySQL `rules` table via SQLAlchemy/PyMySQL.
- **Security:** The actual `.env` file is excluded via `.gitignore` to prevent secret leakage.

### 3.2 Environment & Local LLM Integration (`llm_utils.py`)
Unlike generic AI projects bound to OpenAI, `packet-CRM` is designed for on-premise, secure environments. 
The `get_llm(tier="complex")` factory dynamic routes requests to custom, locally hosted models via `LLM_BASE_URL_COMPLEX` (e.g., `http://localhost:8000/v1`). 
Additionally, it provides a seamless fallback to free Hugging Face endpoints by simply toggling the `USE_HF=true` environment variable.

### 2.2 Core Pipeline (Deterministic StateGraph)
Instead of relying on an unpredictable LLM to orchestrate the subagents, the system uses a highly robust, strictly deterministic Python `StateGraph` (via `langgraph`) in `src/core/agent_orchestrator.py`. This ensures the exact sequential execution of every step.

1. **Log Fetcher Node**: (If `ENABLE_LOG_FETCHING=true`) Automatically triggers the `fetch_elastic_logs` tool to pull relevant Kibana traces using the `eventId`.
2. **Investigator Node**: A React agent equipped with `lookup_rule_by_reason_code` that receives the raw Kafka payload, the Elastic logs, and the database rule configuration to deduce exactly why the failure occurred.
3. **Reviewer Node**: A distinct React agent that acts as a strict QC validator. It evaluates the Investigator's technical analysis.
4. **Conditional Router**: A pure Python control edge that checks the Reviewer's output. If the Reviewer rejects the findings, it forcefully loops back to the Investigator Node with the critique appended. If approved, it routes to Synthesis.
5. **Synthesis Node**: The final agent that takes the approved, heavily vetted technical diagnosis and translates it into a human-readable JSON `Casebook`.

### 3.3 The Agent Ecosystem
The intelligence of the system relies on a multi-agent hierarchy:
- **RejectionManagerAgent**: The conductor. It reads the payload and coordinates a strict sequential pipeline. It cannot solve problems itself.
- **InvestigatorAgent**: The detective. It actively queries databases to correlate error codes (`reasonCode`) with internal business rules (`ruleId`) and determines the technical failure.
- **ReviewerAgent**: The auditor. It checks the Investigator's homework to eliminate hallucinations.
- **SynthesisAgent**: The resolution writer. Once the investigation is validated, this agent synthesizes the findings into plain English, categorizes the remediation steps into strict enums (e.g., `NEW_PACKET`, `REPLAY`), and generates the analytical JSON block.

### 3.4 The Self-Learning Loop (`tool_registry.py`)
If the `ReviewerAgent` spots a mistake (e.g., the Investigator recommended a solution that contradicts the business rule), the Reviewer invokes the `add_learning_rule` tool.
This tool programmatically opens `src/prompts/InvestigatorAgent.md` and permanently writes a new `- CRITICAL RULE:` constraint to the file. This ensures the system perpetually improves its accuracy on future runs without requiring manual developer intervention.

### 3.5 Storage & Casesheets
Outputs are stored in `local_casesheets/casebook_<event_id>/casebook.json`. 
To ensure zero hallucinations, `routes.py` deterministically extracts static metadata directly from the Kafka payload. The output is guaranteed to be a highly structured, hierarchical JSON block formatted for downstream systems:
- **Metadata - Packet Details** (`SRN`, `EID`, `PACKET_TYPE`, etc.)
- **Packet Status** (`Status`, `Service`, `Rejection Data`)
- **Resolution** (Generated by the `SynthesisAgent` containing `Synthesis`, `Action`, `Resident_action`, etc.)

---

## 4. How to Run Locally

1. **Install Dependencies:**
   **Mac/Linux:**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

   **Windows (Command Prompt):**
   ```cmd
   python -m venv .venv
   .venv\Scripts\activate.bat
   pip install -r requirements.txt
   ```

   **Windows (PowerShell):**
   ```powershell
   python -m venv .venv
   .venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   ```

2. **Configuration:**
   Ensure environment variables like `USE_MOCK_DB=true` and `LLM_BASE_URL_COMPLEX` are configured.

3. **Start the Server:**
   ```bash
   python3 src/main.py
   ```
   *Note: This starts both the FastAPI server on port 8000 and the daemon Kafka consumer.*

### 4.2 API Documentation (Swagger UI)
Because the application is built on FastAPI with populated metadata, interactive API documentation is automatically generated.
- Navigate to `http://localhost:8000/docs` to view the Swagger UI.
- Here you can see the fully expanded `MessagePayload` schema (including optional fields like `flowMetaData`, `resubmissionSummary`, etc.) and test the `/process-rejection` endpoint directly from your browser.

4. **Testing Pipeline (No Kafka Required):**
   ```bash
   python3 test_payload.py
   ```
   *This statically parses a mock Kafka payload through the expanded Pydantic models to ensure validation logic is intact.*
