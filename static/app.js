/**
 * Frontend Application Script for Acdyon Resilient Ingestion Dashboard.
 * Handles SSE streaming, real-time log rendering, chaos toggles, and data grid interactions.
 */

document.addEventListener("DOMContentLoaded", () => {
  // Initialize Lucide icons
  if (window.lucide) {
    window.lucide.createIcons();
  }

  // DOM Elements
  const ingestForm = document.getElementById("ingestForm");
  const primarySourceSelect = document.getElementById("primarySourceSelect");
  const fallbackSourceSelect = document.getElementById("fallbackSourceSelect");
  const itemLimitSlider = document.getElementById("itemLimitSlider");
  const limitDisplay = document.getElementById("limitDisplay");
  const startPipelineBtn = document.getElementById("startPipelineBtn");
  const terminalContainer = document.getElementById("terminalContainer");
  const clearLogsBtn = document.getElementById("clearLogsBtn");
  const streamStatusIndicator = document.getElementById("streamStatusIndicator");

  // Stat Counters
  const statTotalJobs = document.getElementById("statTotalJobs");
  const statTotalRuns = document.getElementById("statTotalRuns");
  const statTotalDrifts = document.getElementById("statTotalDrifts");
  const breakerStateBadge = document.getElementById("breakerStateBadge");
  const breakerFailCount = document.getElementById("breakerFailCount");

  // Chaos Lab Toggles
  const chaosRateLimit = document.getElementById("chaosRateLimit");
  const chaosBotBlock = document.getElementById("chaosBotBlock");
  const chaosSchemaDrift = document.getElementById("chaosSchemaDrift");
  const resetChaosBtn = document.getElementById("resetChaosBtn");

  // Jobs Table
  const jobsTableBody = document.getElementById("jobsTableBody");
  const jobSearchInput = document.getElementById("jobSearchInput");
  const refreshJobsBtn = document.getElementById("refreshJobsBtn");

  // Docs Modal
  const docsModal = document.getElementById("docsModal");
  const openDocsModalBtn = document.getElementById("openDocsModalBtn");
  const closeDocsModalBtn = document.getElementById("closeDocsModalBtn");

  // State
  let eventSource = null;

  // Slider update
  itemLimitSlider.addEventListener("input", (e) => {
    limitDisplay.textContent = `${e.target.value} items`;
  });

  // Modal handlers
  openDocsModalBtn.addEventListener("click", () => docsModal.classList.remove("hidden"));
  closeDocsModalBtn.addEventListener("click", () => docsModal.classList.add("hidden"));
  docsModal.addEventListener("click", (e) => {
    if (e.target === docsModal) docsModal.classList.add("hidden");
  });

  // Clear Terminal
  clearLogsBtn.addEventListener("click", () => {
    terminalContainer.innerHTML = '<div class="text-slate-500">[System] Terminal logs cleared.</div>';
  });

  // Append formatted log line to terminal
  function appendLog(eventData) {
    const row = document.createElement("div");
    row.className = "flex items-start space-x-2 text-[11px] leading-tight py-0.5 border-b border-slate-900/60";

    let levelColor = "text-slate-400";
    let badgeBg = "bg-slate-800 text-slate-300";

    if (eventData.level === "SUCCESS") {
      levelColor = "text-emerald-400";
      badgeBg = "bg-emerald-950 text-emerald-300 border border-emerald-800";
    } else if (eventData.level === "WARNING") {
      levelColor = "text-amber-300";
      badgeBg = "bg-amber-950 text-amber-300 border border-amber-800";
    } else if (eventData.level === "ERROR") {
      levelColor = "text-rose-400";
      badgeBg = "bg-rose-950 text-rose-300 border border-rose-800";
    } else if (eventData.stage === "PACING") {
      levelColor = "text-cyan-300";
      badgeBg = "bg-cyan-950 text-cyan-300 border border-cyan-800";
    }

    row.innerHTML = `
      <span class="text-slate-600 font-mono select-none">${eventData.timestamp}</span>
      <span class="px-1.5 py-0.2 rounded text-[9px] font-mono uppercase ${badgeBg}">${eventData.stage}</span>
      ${eventData.source ? `<span class="text-slate-400 font-mono text-[10px]">[${eventData.source}]</span>` : ''}
      <span class="flex-1 ${levelColor}">${escapeHtml(eventData.message)}</span>
    `;

    terminalContainer.appendChild(row);
    terminalContainer.scrollTop = terminalContainer.scrollHeight;
  }

  function escapeHtml(text) {
    const div = document.createElement("div");
    div.textContent = text;
    return div.innerHTML;
  }

  // Trigger Ingestion via SSE
  ingestForm.addEventListener("submit", (e) => {
    e.preventDefault();

    if (eventSource) {
      eventSource.close();
    }

    const primary = primarySourceSelect.value;
    const fallback = fallbackSourceSelect.value;
    const limit = itemLimitSlider.value;

    startPipelineBtn.disabled = true;
    startPipelineBtn.classList.add("opacity-70", "cursor-not-allowed");
    startPipelineBtn.innerHTML = `
      <svg class="animate-spin -ml-1 mr-2 h-4 w-4 text-white" fill="none" viewBox="0 0 24 24">
        <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
        <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z"></path>
      </svg>
      <span>Pacing & Executing Ingestion...</span>
    `;

    streamStatusIndicator.textContent = "STREAMING...";
    streamStatusIndicator.className = "text-cyan-400 font-mono text-[11px] animate-pulse";

    const sseUrl = `/api/ingest?primary=${encodeURIComponent(primary)}&fallback=${encodeURIComponent(fallback)}&limit=${limit}`;
    eventSource = new EventSource(sseUrl);

    eventSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        appendLog(data);

        // Update circuit breaker status if present
        if (data.stage === "PIPELINE_COMPLETE" || data.stage === "PIPELINE_END") {
          finishStream();
        }
      } catch (err) {
        console.error("SSE JSON parse error:", err);
      }
    };

    eventSource.onerror = (err) => {
      console.warn("SSE stream closed or interrupted", err);
      finishStream();
    };
  });

  function finishStream() {
    if (eventSource) {
      eventSource.close();
      eventSource = null;
    }
    startPipelineBtn.disabled = false;
    startPipelineBtn.classList.remove("opacity-70", "cursor-not-allowed");
    startPipelineBtn.innerHTML = `
      <i data-lucide="play" class="w-4 h-4"></i>
      <span>Trigger Resilient Ingestion Run</span>
    `;
    if (window.lucide) window.lucide.createIcons();

    streamStatusIndicator.textContent = "IDLE";
    streamStatusIndicator.className = "text-slate-500 font-mono text-[11px]";

    // Refresh UI stats and jobs table
    fetchStats();
    fetchJobs();
    fetchCircuitStatus();
  }

  // Fetch Summary Statistics
  async function fetchStats() {
    try {
      const res = await fetch("/api/stats");
      const data = await res.json();
      if (data.metrics) {
        statTotalJobs.textContent = data.metrics.total_jobs;
        statTotalRuns.textContent = data.metrics.total_runs;
        statTotalDrifts.textContent = data.metrics.total_drifts;
      }
    } catch (err) {
      console.error("Failed to fetch stats:", err);
    }
  }

  // Fetch Circuit Breaker Status
  async function fetchCircuitStatus() {
    try {
      const res = await fetch("/api/circuit-status");
      const data = await res.json();
      const primary = primarySourceSelect.value;
      const status = data[primary] || Object.values(data)[0];

      if (status) {
        if (status.state === "CLOSED") {
          breakerStateBadge.textContent = "CLOSED (HEALTHY)";
          breakerStateBadge.className = "px-2.5 py-1 rounded text-xs font-bold font-mono bg-emerald-950 text-emerald-400 border border-emerald-700 badge-glow-emerald";
        } else if (status.state === "OPEN") {
          breakerStateBadge.textContent = "OPEN (TRIPPED / COOLDOWN)";
          breakerStateBadge.className = "px-2.5 py-1 rounded text-xs font-bold font-mono bg-rose-950 text-rose-400 border border-rose-700 badge-glow-rose";
        } else {
          breakerStateBadge.textContent = "HALF_OPEN (CANARY PROBE)";
          breakerStateBadge.className = "px-2.5 py-1 rounded text-xs font-bold font-mono bg-amber-950 text-amber-400 border border-amber-700 badge-glow-amber";
        }
        breakerFailCount.textContent = `Failures: ${status.failure_count} / ${status.failure_threshold}`;
      }
    } catch (err) {
      console.error("Failed to fetch circuit status:", err);
    }
  }

  // Fetch Jobs and render data grid
  async function fetchJobs() {
    try {
      const query = jobSearchInput.value.trim();
      const url = `/api/jobs?limit=50${query ? `&search=${encodeURIComponent(query)}` : ''}`;
      const res = await fetch(url);
      const data = await res.json();

      jobsTableBody.innerHTML = "";
      if (!data.jobs || data.jobs.length === 0) {
        jobsTableBody.innerHTML = `
          <tr>
            <td colspan="6" class="px-4 py-8 text-center text-slate-500">
              No jobs found in SQLite database. Trigger an ingestion run above!
            </td>
          </tr>
        `;
        return;
      }

      data.jobs.forEach((job) => {
        const tr = document.createElement("tr");
        tr.className = "hover:bg-slate-800/40 transition";

        const tagsHtml = (job.tags || []).map(t => 
          `<span class="inline-block px-2 py-0.5 rounded text-[10px] font-mono bg-slate-800 text-slate-300 border border-slate-700 mr-1 mb-1">${escapeHtml(t)}</span>`
        ).join("");

        tr.innerHTML = `
          <td class="px-4 py-3">
            <div class="font-semibold text-slate-100">${escapeHtml(job.title)}</div>
            <div class="text-slate-400 text-[11px]">${escapeHtml(job.company)}</div>
          </td>
          <td class="px-4 py-3">
            <span class="px-2 py-0.5 rounded text-[10px] font-mono bg-cyan-950 text-cyan-300 border border-cyan-800">${escapeHtml(job.source)}</span>
          </td>
          <td class="px-4 py-3">
            <div class="text-slate-300">${escapeHtml(job.location || 'Remote')}</div>
            <div class="text-emerald-400 text-[11px] font-mono">${escapeHtml(job.salary || 'Unspecified')}</div>
          </td>
          <td class="px-4 py-3 max-w-[200px]">
            <div class="flex flex-wrap">${tagsHtml || '<span class="text-slate-500 text-[10px]">—</span>'}</div>
          </td>
          <td class="px-4 py-3 font-mono text-slate-400 text-[10px]">
            ${job.ingested_at.replace("T", " ").substring(0, 19)}
          </td>
          <td class="px-4 py-3 text-right">
            <a href="${escapeHtml(job.url)}" target="_blank" rel="noopener noreferrer" class="inline-flex items-center gap-1 text-cyan-400 hover:text-cyan-300 font-medium text-xs hover:underline">
              <span>View</span>
              <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"></path></svg>
            </a>
          </td>
        `;
        jobsTableBody.appendChild(tr);
      });
    } catch (err) {
      console.error("Failed to fetch jobs:", err);
    }
  }

  // Chaos Lab Toggle Handler
  async function syncChaosConfig() {
    try {
      await fetch("/api/chaos", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          rate_limit: chaosRateLimit.checked,
          bot_block: chaosBotBlock.checked,
          schema_drift: chaosSchemaDrift.checked,
          latency: 0.0
        })
      });
    } catch (err) {
      console.error("Failed to sync chaos settings:", err);
    }
  }

  chaosRateLimit.addEventListener("change", syncChaosConfig);
  chaosBotBlock.addEventListener("change", syncChaosConfig);
  chaosSchemaDrift.addEventListener("change", syncChaosConfig);

  resetChaosBtn.addEventListener("click", async () => {
    chaosRateLimit.checked = false;
    chaosBotBlock.checked = false;
    chaosSchemaDrift.checked = false;
    await syncChaosConfig();
    
    // Reset all breakers
    for (const src of ["SandboxSource", "RemoteOK", "WeWorkRemotely"]) {
      await fetch("/api/circuit-reset", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ source: src })
      });
    }
    fetchCircuitStatus();
  });

  // Search filter
  jobSearchInput.addEventListener("input", () => {
    fetchJobs();
  });
  refreshJobsBtn.addEventListener("click", () => {
    fetchStats();
    fetchJobs();
    fetchCircuitStatus();
  });
  primarySourceSelect.addEventListener("change", fetchCircuitStatus);

  // Initial load
  fetchStats();
  fetchJobs();
  fetchCircuitStatus();
});
