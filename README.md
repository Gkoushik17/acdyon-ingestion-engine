# Acdyon Technologies — Resilient Data Ingestion & Anti-Detection System

> **Frontend / Systems Engineering Challenge (Part 1)**  
> *"Build It Like You Mean It"*

A production-grade, fault-tolerant job data ingestion engine built with **Python, FastAPI, HTTPX, Pydantic, and SQLite**. Demonstrates resilience against bot detection, rate limits, schema drift, and network failures with real-time SSE log streaming and an interactive monitoring dashboard.

---

## 🌟 Key Engineering Capabilities

1. **Anti-Detection Pacing & Jitter:** Token bucket rate limiter with randomized exponential backoff ($base \cdot 2^{attempt} + uniform(0.1, 1.5)$) preventing harmonic frequency detection.
2. **Browser Fingerprint Normalization:** Direct HTTP emulation with modern `Sec-CH-UA`, `Sec-Fetch-*`, and realistic browser header orders.
3. **3-State Circuit Breaker (`CLOSED` $\leftrightarrow$ `OPEN` $\leftrightarrow$ `HALF_OPEN`):** Automatically detects `429` (Rate Limited) or `403` (Bot Challenge) responses, short-circuiting to fallback public feeds without getting IPs banned.
4. **Schema Drift Telemetry (Zero Silent Failures):** Validates required fields using Pydantic V2 and BeautifulSoup4; anomalous/corrupted markup is flagged and logged to telemetry without pipeline crash.
5. **Interactive Chaos Lab:** Allows evaluators to simulate `429`, `403`, and broken markup live in the UI to watch failover and resilience mechanisms in real time.
6. **Deduplicated SQLite Storage:** Computes deterministic SHA256 hashes across heterogeneous sources to guarantee idempotent database insertion.

---

## 🚀 Quick Start (Local)

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Run the Server
```bash
python main.py
```
Visit **[http://localhost:8000](http://localhost:8000)** in your browser.

---

## 🌐 Free Cloud Deployment (Render / Railway / VPS)

### Deploy on Render in 2 Minutes:
1. Push this repository to your GitHub account.
2. Log into [Render.com](https://render.com) and click **New + > Web Service**.
3. Select your GitHub repository.
4. Set:
   - **Environment:** `Python 3`
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `uvicorn main:app --host 0.0.0.0 --port $PORT`
5. Click **Deploy Web Service** to get your public Live URL.

---

## 📂 Project Structure

```
Acdyon_assessment/
├── engine/ / services/
│   ├── ingestion.py       # Orchestrator & SSE streaming engine
│   ├── resilience.py      # Pacer, Header Pool, Circuit Breaker, Chaos Engine
│   ├── sources.py         # Multi-source registry (RemoteOK, WWR, Sandbox)
│   └── normalizer.py      # HTML sanitizer, Pydantic validation & drift scorer
├── static/
│   ├── app.js             # Live SSE streaming & UI interactions
│   └── styles.css         # Dark theme & styling
├── templates/
│   └── index.html         # Real-time monitoring dashboard & docs explorer
├── database.py            # SQLite thread-safe database layer
├── models.py              # Pydantic schemas & telemetry models
├── main.py                # FastAPI server entrypoint
├── DECISIONS.md           # 1-page required submission reflection document
├── DESIGN_DOC.md          # Comprehensive architectural design document
├── render.yaml            # 1-click cloud deploy blueprint
├── Dockerfile             # Container configuration
└── requirements.txt       # Python dependencies
```

---

## 📄 Submission Documents
- [1-Page DECISIONS.md](./DECISIONS.md) — Answers prompt questions regarding architecture choices, trade-offs, and AI usage.
- [Detailed System Design Document](./DESIGN_DOC.md) — Deep-dive covering detection surface, ingestion strategy, resilience, and ToS lines.
