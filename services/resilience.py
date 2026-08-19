"""
Anti-Detection & System Resilience Service.
Implements:
1. Pacer & Jitter Rate Limiter (Token bucket + randomized human jitter).
2. Browser Header / TLS Fingerprint Normalizer.
3. 3-State Circuit Breaker (CLOSED -> OPEN -> HALF_OPEN).
4. Chaos Simulator for live testing of 429, 403, and network failures.
"""

import asyncio
import random
import time
from typing import Dict, Optional, Any, Callable
from models import CircuitBreakerStatus


class BrowserFingerprintPool:
    """
    Simulates real browser TLS / HTTP header footprints.
    Prevents simplistic bot flagging based on missing Sec-CH-UA or generic python-httpx User-Agents.
    """
    PROFILES = [
        {
            "name": "Chrome_122_MacOS",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Sec-CH-UA": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"macOS"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1"
        },
        {
            "name": "Chrome_121_Windows",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Sec-CH-UA": '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1"
        },
        {
            "name": "Firefox_123_Windows",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1"
        }
    ]

    @classmethod
    def get_random_headers(cls) -> Dict[str, str]:
        profile = random.choice(cls.PROFILES)
        headers = {k: v for k, v in profile.items() if k != "name"}
        return headers


class JitterPacer:
    """
    Token-bucket style pacer that introduces humanized jitter to prevent
    fixed-interval request cadences (a primary trigger for heuristic bot blockers).
    """
    def __init__(self, base_delay_seconds: float = 1.0, max_jitter_seconds: float = 1.5):
        self.base_delay = base_delay_seconds
        self.max_jitter = max_jitter_seconds

    async def pace(self, attempt: int = 0) -> float:
        """
        Calculates and awaits exponential backoff with full randomized jitter.
        Formula: delay = base_delay * (2 ^ attempt) + uniform(0.1, max_jitter)
        """
        backoff = self.base_delay * (2 ** min(attempt, 4))
        jitter = random.uniform(0.1, self.max_jitter)
        total_delay = backoff + jitter
        await asyncio.sleep(total_delay)
        return total_delay


class CircuitBreaker:
    """
    3-State Circuit Breaker to protect client IP reputation.
    
    States:
    - CLOSED: Normal operation. Requests flow freely.
    - OPEN: Target is failing/blocking (e.g. 429/403). Requests immediately short-circuit to fallback.
    - HALF_OPEN: Cooldown expired. Allows a single canary probe to test if target is healthy.
    """
    def __init__(self, source_name: str, failure_threshold: int = 2, recovery_cooldown_seconds: float = 15.0):
        self.source_name = source_name
        self.failure_threshold = failure_threshold
        self.recovery_cooldown = recovery_cooldown_seconds
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.last_failure_reason: Optional[str] = None
        self.tripped_at_iso: Optional[str] = None

    def allow_request(self) -> bool:
        """Checks if a request is permitted to hit the target source."""
        now = time.time()
        if self.state == "OPEN":
            # Check if cooldown has elapsed to enter HALF_OPEN
            if self.last_failure_time and (now - self.last_failure_time >= self.recovery_cooldown):
                self.state = "HALF_OPEN"
                return True
            return False
        return True

    def record_success(self):
        """Records a successful request, resetting state to CLOSED."""
        self.failure_count = 0
        self.state = "CLOSED"
        self.last_failure_reason = None

    def record_failure(self, reason: str):
        """Increments failure count and trips to OPEN if threshold exceeded."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        self.last_failure_reason = reason

        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"
            self.tripped_at_iso = time.strftime("%Y-%m-%d %H:%M:%S")

    def reset(self):
        """Manually resets the breaker."""
        self.state = "CLOSED"
        self.failure_count = 0
        self.last_failure_time = None
        self.last_failure_reason = None
        self.tripped_at_iso = None

    def get_status(self) -> CircuitBreakerStatus:
        return CircuitBreakerStatus(
            source=self.source_name,
            state=self.state,
            failure_count=self.failure_count,
            failure_threshold=self.failure_threshold,
            recovery_time_seconds=self.recovery_cooldown,
            last_failure_reason=self.last_failure_reason,
            tripped_at=self.tripped_at_iso
        )


class ChaosEngine:
    """
    Allows toggling simulated failures (429 Rate Limits, 403 Bot Blocks, 500 Errors, Schema Drift)
    to prove live pipeline resilience without risking real third-party bans.
    """
    def __init__(self):
        self.simulate_rate_limit = False   # 429
        self.simulate_bot_block = False    # 403
        self.simulate_schema_drift = False # Corrupt markup
        self.simulated_latency_sec = 0.0

    def configure(self, rate_limit: bool = False, bot_block: bool = False, 
                  schema_drift: bool = False, latency: float = 0.0):
        self.simulate_rate_limit = rate_limit
        self.simulate_bot_block = bot_block
        self.simulate_schema_drift = schema_drift
        self.simulated_latency_sec = max(0.0, latency)

    def get_status(self) -> Dict[str, Any]:
        return {
            "simulate_rate_limit_429": self.simulate_rate_limit,
            "simulate_bot_block_403": self.simulate_bot_block,
            "simulate_schema_drift": self.simulate_schema_drift,
            "simulated_latency_sec": self.simulated_latency_sec
        }


# Global shared instances for application
global_pacer = JitterPacer(base_delay_seconds=0.6, max_jitter_seconds=1.2)
circuit_breakers: Dict[str, CircuitBreaker] = {
    "RemoteOK": CircuitBreaker("RemoteOK", failure_threshold=2, recovery_cooldown_seconds=10.0),
    "WeWorkRemotely": CircuitBreaker("WeWorkRemotely", failure_threshold=2, recovery_cooldown_seconds=10.0),
    "SandboxSource": CircuitBreaker("SandboxSource", failure_threshold=2, recovery_cooldown_seconds=8.0)
}
global_chaos = ChaosEngine()
