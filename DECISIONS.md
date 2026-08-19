# Written Explanation & Decisions (`DECISIONS.md`)
**Assessment Track:** Part 1 — Resilient Ingestion & Anti-Detection System  
**Author / Candidate Submission**

---

### 1. Why this ingestion strategy over the obvious alternative you rejected?

#### The Obvious Alternative Rejected:
The naive industry approach to web scraping is spinning up headless browser clusters (e.g., standard Playwright/Puppeteer instances) to render full client-side SPA bundles, or firing un-paced, raw HTTP concurrency loops using naive scripts.

#### Why It Was Rejected:
1. **High Detection Surface:** Standard headless Chromium instances leak distinctive browser fingerprints (e.g., `navigator.webdriver = true`, missing Chrome plugins, telltale WebGL/Canvas rendering artifacts, predictable JA3/TLS cipher signatures, and missing client-hint headers). Anti-bot systems (Cloudflare Turnstile, Akamai Bot Manager, Datadome) flag these within minutes.
2. **Resource Inefficiency & Fragility:** Spawning headless browsers introduces extreme CPU/memory overhead (~150MB+ per tab), slow page rendering latencies, and high crash vulnerability under concurrent loads.
3. **Robotic Cadence Signature:** Firing requests at fixed programmatic intervals (e.g., exactly every 1000ms) triggers heuristic rate-limiters regardless of proxy quality.

#### The Chosen Strategy:
We engineered an **Asynchronous Adaptive Ingestion Engine** powered by `httpx`, `asyncio`, and `Pydantic`:
- **Token Bucket Pacing with Full Exponential Jitter:** Every request applies randomized humanized delay ($d = \text{base} \cdot 2^{\text{attempt}} + \text{uniform}(0.1, \text{jitter})$), preventing rhythmic timing flags.
- **Browser Fingerprint Normalization:** Direct HTTP emulation with modern `Sec-CH-UA`, `Sec-Fetch-*`, and realistic header ordering without browser execution overhead.
- **3-State Circuit Breaker (`CLOSED` $\leftrightarrow$ `OPEN` $\leftrightarrow$ `HALF_OPEN`):** Immediately trips to fallback sources upon receiving `429` (Too Many Requests) or `403` (Bot Challenge), preserving IP reputation.
- **Zero Silent Failure Normalizer:** Validates fields via Pydantic; schema drifts trigger structured telemetry rather than killing the pipeline or corrupting data.

---

### 2. One trade-off made under the time limit, and what you'd do with a real week.

#### The Time-Limit Trade-off:
To guarantee 100% deterministic local and cloud deployment without external proxy vendor credentials or paid CAPTCHA solvers, we implemented **in-memory circuit breaking, multi-source failover routing, and single-node SQLite persistence**.

#### What I Would Build with a Full Week:
1. **Distributed Proxy Mesh & Session Pool:** Integrate residential/mobile proxy rotation (BrightData/Oxylabs) with sticky session management, cookie persistence, and automatic IP health scoring.
2. **TLS / JA4 Fingerprint Spoofing (Curl-Impersonate):** Bind Python `httpx` to native `curl-impersonate` / `BoringSSL` C-bindings to replicate byte-exact Chrome and Safari TLS Client Hello handshakes (cipher suites, extensions, elliptic curves).
3. **Autonomous LLM-Assisted DOM Repair:** When Pydantic flags schema drift ($>0.4$ drift score), trigger a background worker that feeds the raw mutated DOM into a localized LLM (e.g., Gemini Flash / Llama 3) to dynamically generate new CSS/XPath selectors and self-heal the parser without human intervention.
4. **Distributed Task Queue:** Migrate SQLite and in-memory orchestrator to Redis + Celery/ARQ with PostgreSQL and Dead-Letter Queues (DLQ).

---

### 3. Where did you use AI tools, and what did you personally verify or change afterward?

#### AI Utilization:
- Utilized AI agentic tools for initial boilerplating of Pydantic model definitions, synthetic mock dataset structure, and crafting modern Tailwind CSS dashboard styling.

#### What Was Personally Verified, Architected, and Changed:
1. **Mathematical Jitter & Pacing Logic:** Personally verified and calibrated the backoff formula ($base \cdot 2^n + uniform$) to ensure requests avoid harmonic frequency patterns.
2. **Circuit Breaker State Transitions:** Replaced simplistic try/catch blocks with a formal 3-state state machine supporting canary probes in `HALF_OPEN` state with automatic cooldown timers.
3. **HTML Sanitization & Safety:** Audited the `BeautifulSoup4` cleaning pipeline to ensure malicious `<script>`, `<iframe>`, and `<style>` injection vectors are explicitly decomposed before text summarization and database insertion.
4. **Scope Guardrail Adherence:** Verified that live connections exclusively target low-risk public RSS/API endpoints and our controlled sandbox, preventing accidental ToS violations on live proprietary platforms.
