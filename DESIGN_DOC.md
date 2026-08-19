# System Design Document: Resilient Ingestion & Anti-Detection Architecture
**Project:** Acdyon Technologies Engineering Assessment (Part 1)  
**Author:** Candidate Submission  
**Stack:** Python 3.10+, FastAPI, HTTPX, AsyncIO, BeautifulSoup4, Feedparser, Pydantic V2, SQLite

---

## 1. System Architecture Diagram

```
                             FastAPI Service
                                    │
          ┌─────────────────────────┴─────────────────────────┐
          │                                                   │
     Dashboard UI / REST API                              Ingestion Orchestrator
  (/api/ingest, /api/jobs, SSE)                               │
                                                              ▼
                                                   Token Bucket Jitter Pacer
                                                              │
                                                              ▼
                                                   Browser Fingerprint Pool
                                                              │
                                                              ▼
                                                    3-State Circuit Breaker
                                                              │
                                                ┌─────────────┴─────────────┐
                                                │                           │
                                         [CLOSED / Canary]               [OPEN]
                                                │                           │
                                                ▼                           ▼
                                         Primary Target             Fallback Source
                                       (e.g., RemoteOK)        (e.g., WeWorkRemotely / Sandbox)
                                                │                           │
                                                └─────────────┬─────────────┘
                                                              │
                                                              ▼
                                                    Pydantic Normalizer
                                                   & Schema Drift Engine
                                                              │
                                                ┌─────────────┴─────────────┐
                                                │                           │
                                           [Valid Data]             [Drift Detected]
                                                │                           │
                                                ▼                           ▼
                                          SQLite Job Store         Telemetry Logger
```

---

## 2. In-Depth Engineering Analysis

### 2.1. Detection Surface — What Gives Automated Clients Away
Modern anti-bot systems (Cloudflare Bot Management, Akamai, PerimeterX, Datadome) deploy multi-layer inspection:

1. **TCP / TLS / HTTP2 Fingerprinting (JA3 / JA4):**
   - Standard Python libraries (`urllib`, vanilla `requests`) transmit default OpenSSL cipher suites, signature algorithms, and extension orders that immediately flag the client as non-browser automation.
   - *Design Accounted For:* Emulates modern browser header order, `Sec-CH-UA` client hints, and allows pluggable TLS cipher configuration.

2. **Request Cadence & Behavioral Heuristics:**
   - Robotic actors execute on rigid programmatic intervals (e.g. exactly 1.0s or 500ms loops).
   - *Design Accounted For:* Token bucket algorithm with randomized exponential jitter ($base \cdot 2^{attempt} + uniform(0.1, 1.5)$) breaking any harmonic frequency patterns.

3. **HTTP Client Hints & Header Inconsistencies:**
   - Missing `Sec-Fetch-Dest`, `Sec-Fetch-Mode`, `Sec-Fetch-Site`, or mismatched `User-Agent` vs `Sec-CH-UA` versions immediately trigger challenge captchas.
   - *Design Accounted For:* Comprehensive profile matching ensuring User-Agent strings and Sec-CH-UA platform tags are 100% coherent.

4. **DOM & Canvas Fingerprinting (for Headless Browsers):**
   - Headless Chromium leaks `window.navigator.webdriver = true`, missing audio context variance, and distinctive WebGL renderer strings.
   - *Design Accounted For:* Bypasses heavy headless browsers entirely where possible via direct, normalized HTTP ingestion.

---

### 2.2. Ingestion Strategy & Multi-Source Failover

1. **Pacing & Rate Limiting:**
   - Implements exponential backoff on retry. Requests are distributed across time windows rather than burst-loaded.

2. **3-State Circuit Breaker Pattern:**
   - **`CLOSED`**: Requests proceed normally to the primary origin.
   - **`OPEN`**: When $N$ consecutive failures (HTTP 429, 403, or connection timeouts) occur, the circuit trips `OPEN` for a 15-second cooldown window. All incoming requests immediately route to the secondary fallback source without touching the primary host.
   - **`HALF_OPEN`**: Following cooldown expiry, a single canary request tests origin health. If successful, resets to `CLOSED`; if failing, re-opens cooldown.

3. **Plan B Architecture (When Primary Shuts Down):**
   - Ingestion is abstracted via a polymorphic `BaseSourceFetcher` registry.
   - If a primary platform revamps its API or enforces aggressive CAPTCHAs overnight, the pipeline automatically routes through secondary feeds, syndicated RSS endpoints, or cached data snapshots without human intervention or pipeline failure.

---

### 2.3. Resilience — Zero Silent Failures & Schema Drift

1. **Schema Contracts with Pydantic V2:**
   - Mandatory validation for `title`, `company`, `url`, and sanitized `description_snippet`.
2. **Schema Drift Telemetry:**
   - If an origin alters its HTML markup or JSON structure, missing DOM selectors are trapped:
     - The batch does not crash.
     - Unparseable items generate a `SchemaDriftReport` recording drift scores, missing keys, and sample raw snippets in SQLite.
     - Alerts are pushed live over Server-Sent Events (SSE).
3. **Deterministic Deduplication:**
   - Canonical IDs are computed via SHA256 hashes (`source:title:company:url`). Duplicate items across runs or multiple feeds are skipped silently via SQLite `INSERT` constraints without polluting the database.

---

### 2.4. Where We Stop — Ethical & Technical Boundaries

1. **Robots.txt & Rate Limit Respect:**
   - We strictly honor `Crawl-Delay` and `robots.txt` directives.
   - When a platform signals `HTTP 429` or requests backoff, our system ceases requests immediately rather than cycling aggressive proxy pools to bypass defensive measures.
2. **Scope Guardrails:**
   - In production, we never scrape behind authenticated user logins, paywalls, or private personal data (PII).
   - Our demo connects to open public RSS feeds and controlled sandboxes as stipulated in the assessment requirements.
3. **The Technical & Personal Line:**
   - Automated ingestion is intended for public, syndicated indexation. When a platform explicitly demands commercial API licensing or deploys CAPTCHAs, ethical engineering dictates transitioning to official partnership APIs rather than adversarial circumvention.

---

## 3. Local Execution & Deployment

### Run Locally:
```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Start FastAPI Server
python main.py
```
Open [http://localhost:8000](http://localhost:8000) in your browser.

### Deploy to Render / Railway / Free VPS:
- Build Command: `pip install -r requirements.txt`
- Start Command: `uvicorn main:app --host 0.0.0.0 --port $PORT`
