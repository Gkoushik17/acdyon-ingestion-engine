"""
Source fetchers for public job feeds & sandbox endpoint.
Implements:
1. Source A: RemoteOK Public API/Feed
2. Source B: WeWorkRemotely RSS/HTML Feed
3. Sandbox Source: Fully controlled sandbox adhering to prompt scope guardrails
"""

import httpx
import feedparser
import asyncio
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Tuple
from models import RawJobItem
from services.resilience import BrowserFingerprintPool, global_chaos


class BaseSourceFetcher:
    """Abstract base source fetcher."""
    name: str = "BaseSource"

    async def fetch(self, limit: int = 15) -> List[RawJobItem]:
        raise NotImplementedError


class RemoteOKFetcher(BaseSourceFetcher):
    """
    Source A: RemoteOK Public Developer Feed.
    Pulls structured remote job data using httpx and rotating browser headers.
    """
    name = "RemoteOK"
    API_URL = "https://remoteok.com/api"

    async def fetch(self, limit: int = 15) -> List[RawJobItem]:
        headers = BrowserFingerprintPool.get_random_headers()
        # RemoteOK requires custom User-Agent to avoid generic 403
        headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            response = await client.get(self.API_URL, headers=headers)
            
            if response.status_code == 429:
                raise RuntimeError(f"Rate limited by {self.name} (HTTP 429)")
            elif response.status_code == 403:
                raise RuntimeError(f"Anti-bot block triggered by {self.name} (HTTP 403)")
            elif response.status_code != 200:
                raise RuntimeError(f"Failed to fetch from {self.name} with status {response.status_code}")

            data = response.json()
            # First item in RemoteOK is legal/metadata disclaimer
            raw_items = [item for item in data if isinstance(item, dict) and "position" in item][:limit]

            results = []
            for item in raw_items:
                results.append(RawJobItem(
                    source_name=self.name,
                    raw_title=item.get("position"),
                    raw_company=item.get("company"),
                    raw_location=item.get("location") or "Remote",
                    raw_description=item.get("description"),
                    raw_url=item.get("url") or f"https://remoteok.com/l/{item.get('id')}",
                    raw_salary=f"${item.get('salary_min', '')} - ${item.get('salary_max', '')}" if item.get('salary_min') else None,
                    raw_tags=item.get("tags") or [],
                    raw_published_at=item.get("date")
                ))
            return results


class WeWorkRemotelyFetcher(BaseSourceFetcher):
    """
    Source B: WeWorkRemotely RSS / XML Feed.
    Parsed via feedparser and BeautifulSoup.
    """
    name = "WeWorkRemotely"
    RSS_URL = "https://weworkremotely.com/categories/remote-programming-jobs.rss"

    async def fetch(self, limit: int = 15) -> List[RawJobItem]:
        headers = BrowserFingerprintPool.get_random_headers()
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            response = await client.get(self.RSS_URL, headers=headers)
            
            if response.status_code == 429:
                raise RuntimeError(f"Rate limited by {self.name} (HTTP 429)")
            elif response.status_code == 403:
                raise RuntimeError(f"Anti-bot block triggered by {self.name} (HTTP 403)")
            elif response.status_code != 200:
                raise RuntimeError(f"HTTP error {response.status_code} fetching from {self.name}")

            parsed = feedparser.parse(response.text)
            entries = parsed.entries[:limit]

            results = []
            for entry in entries:
                # WWR titles are often formatted as "Company: Title"
                title_raw = entry.get("title", "")
                company = "WeWorkRemotely Job"
                title = title_raw
                if ":" in title_raw:
                    parts = title_raw.split(":", 1)
                    company = parts[0].strip()
                    title = parts[1].strip()

                # Clean summary
                summary = entry.get("summary", "")
                
                results.append(RawJobItem(
                    source_name=self.name,
                    raw_title=title,
                    raw_company=company,
                    raw_location="Remote",
                    raw_description=summary,
                    raw_url=entry.get("link"),
                    raw_tags=["Programming", "Remote"],
                    raw_published_at=entry.get("published")
                ))
            return results


class SandboxFetcher(BaseSourceFetcher):
    """
    Controlled Sandbox Source.
    Adheres to the prompt scope guardrail.
    Simulates realistic job ingestion and responds dynamically to chaos flags
    (e.g. 429 Rate Limits, 403 Bot Blocks, and Schema Mutations).
    """
    name = "SandboxSource"

    MOCK_JOBS = [
        {
            "title": "Senior Frontend Systems Engineer",
            "company": "Acdyon Technologies",
            "location": "Remote / Bengaluru",
            "salary": "$130,000 - $160,000",
            "tags": ["React", "TypeScript", "Performance", "Architecture"],
            "url": "https://acdyon.dev/careers/fe-systems",
            "desc": "Lead frontend architecture and high-throughput real-time streaming dashboards."
        },
        {
            "title": "Distributed Systems & Ingestion Engineer",
            "company": "Acdyon Cloud Labs",
            "location": "Remote",
            "salary": "$140,000 - $175,000",
            "tags": ["Python", "FastAPI", "AsyncIO", "Resilience"],
            "url": "https://acdyon.dev/careers/ingestion-systems",
            "desc": "Build resilient data extraction pipelines surviving aggressive rate limits and bot heuristics."
        },
        {
            "title": "Full Stack Product Developer",
            "company": "Nexus Scale AI",
            "location": "Remote / San Francisco",
            "salary": "$150,000 - $190,000",
            "tags": ["Next.js", "Python", "Tailwind", "PostgreSQL"],
            "url": "https://nexus-scale.ai/jobs/fs-product",
            "desc": "Architect AI agent interfaces and resilient ingestion pipelines."
        },
        {
            "title": "Staff Reliability & Security Engineer",
            "company": "Vanguard Data",
            "location": "London / Remote",
            "salary": "$125,000 - $155,000",
            "tags": ["Security", "Proxies", "TLS", "Anti-Bot"],
            "url": "https://vanguard-data.io/jobs/reliability-eng",
            "desc": "Designing defensive ingestion perimeters and ethical data pipelines."
        }
    ]

    async def fetch(self, limit: int = 15) -> List[RawJobItem]:
        # Respect simulated latency
        if global_chaos.simulated_latency_sec > 0:
            await asyncio.sleep(global_chaos.simulated_latency_sec)

        # Check Chaos conditions
        if global_chaos.simulate_rate_limit:
            raise RuntimeError("HTTP 429: Too Many Requests (Rate limit threshold reached on primary origin)")

        if global_chaos.simulate_bot_block:
            raise RuntimeError("HTTP 403: Bot Protection Intercept (Cloudflare Turnstile / PerimeterX Challenge Detected)")

        results = []
        for i, item in enumerate(self.MOCK_JOBS[:limit]):
            if global_chaos.simulate_schema_drift and i == 0:
                # Mutate schema for drift detection testing: missing title, wrong type
                results.append(RawJobItem(
                    source_name=self.name,
                    raw_title=None,  # Missing mandatory title!
                    raw_company=None, # Missing mandatory company!
                    raw_location="Unknown Location",
                    raw_description="<corrupted payload snippet> missing required DOM selector .job-card-title",
                    raw_url="https://acdyon.dev/corrupted-item",
                    raw_tags=[]
                ))
            else:
                results.append(RawJobItem(
                    source_name=self.name,
                    raw_title=item["title"],
                    raw_company=item["company"],
                    raw_location=item["location"],
                    raw_description=f"<p>{item['desc']}</p><p>Requires deep systems understanding.</p>",
                    raw_url=item["url"],
                    raw_salary=item.get("salary"),
                    raw_tags=item["tags"],
                    raw_published_at="Today"
                ))
        return results


# Registry of available sources
SOURCES_REGISTRY: Dict[str, BaseSourceFetcher] = {
    "RemoteOK": RemoteOKFetcher(),
    "WeWorkRemotely": WeWorkRemotelyFetcher(),
    "SandboxSource": SandboxFetcher()
}
