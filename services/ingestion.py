"""
Ingestion Service Orchestrator.
Coordinates:
1. Pacing & Jitter execution
2. Circuit Breaker checks
3. Multi-source fallback (Primary Source -> Fallback Source)
4. Normalization & Schema validation
5. Deduplicated SQLite storage
6. Real-time Server-Sent Events (SSE) streaming
"""

import asyncio
import json
import uuid
from datetime import datetime
from typing import AsyncGenerator, Dict, Any, Optional

from models import IngestionProgressEvent, RawJobItem
from services.resilience import global_pacer, circuit_breakers
from services.sources import SOURCES_REGISTRY
from services.normalizer import DataNormalizer
from database import save_jobs, record_run


class IngestionOrchestrator:
    """Manages resilient multi-source pipeline executions."""

    @classmethod
    async def run_pipeline_stream(
        cls, 
        primary_source_name: str = "SandboxSource",
        fallback_source_name: str = "WeWorkRemotely",
        item_limit: int = 10
    ) -> AsyncGenerator[str, None]:
        """
        Executes the ingestion pipeline and yields SSE formatted event strings.
        """
        run_id = f"run_{uuid.uuid4().hex[:8]}"
        started_at = datetime.utcnow().isoformat()

        def make_sse(event: IngestionProgressEvent) -> str:
            return f"data: {event.model_dump_json()}\n\n"

        yield make_sse(IngestionProgressEvent(
            stage="INIT",
            level="INFO",
            message=f"Pipeline run initialized (Run ID: {run_id}). Primary: {primary_source_name}, Fallback: {fallback_source_name}",
            metadata={"run_id": run_id, "primary": primary_source_name, "fallback": fallback_source_name}
        ))

        primary_breaker = circuit_breakers.get(primary_source_name)
        fallback_breaker = circuit_breakers.get(fallback_source_name)

        active_source_name = primary_source_name
        fallback_used = False
        raw_items = []
        max_retries = 2

        # Step 1: Check Primary Circuit Breaker
        if primary_breaker and not primary_breaker.allow_request():
            yield make_sse(IngestionProgressEvent(
                stage="CIRCUIT_CHECK",
                level="WARNING",
                source=primary_source_name,
                message=f"Circuit Breaker is OPEN for {primary_source_name} (Cooldown active). Short-circuiting to fallback: {fallback_source_name}",
                metadata={"breaker_state": primary_breaker.state}
            ))
            active_source_name = fallback_source_name
            fallback_used = True

        # Step 2: Ingestion Loop with Adaptive Pacing & Jitter
        fetch_success = False
        for attempt in range(max_retries + 1):
            source_fetcher = SOURCES_REGISTRY.get(active_source_name)
            if not source_fetcher:
                yield make_sse(IngestionProgressEvent(
                    stage="ERROR",
                    level="ERROR",
                    message=f"Unknown source connector: {active_source_name}"
                ))
                break

            # Pacing & Jitter
            yield make_sse(IngestionProgressEvent(
                stage="PACING",
                level="INFO",
                source=active_source_name,
                message=f"Applying randomized human jitter pacer (Attempt {attempt + 1}/{max_retries + 1})...",
                metadata={"attempt": attempt}
            ))
            delay_applied = await global_pacer.pace(attempt=attempt)
            yield make_sse(IngestionProgressEvent(
                stage="PACING",
                level="INFO",
                source=active_source_name,
                message=f"Pacer delay satisfied ({delay_applied:.2f}s elapsed). Rotating browser TLS headers...",
                metadata={"delay_sec": round(delay_applied, 2)}
            ))

            # Fetch attempt
            try:
                yield make_sse(IngestionProgressEvent(
                    stage="FETCHING",
                    level="INFO",
                    source=active_source_name,
                    message=f"Executing async request against {active_source_name} origin...",
                    metadata={"target": active_source_name}
                ))
                raw_items = await source_fetcher.fetch(limit=item_limit)
                
                # Success on active source
                if active_source_name in circuit_breakers:
                    circuit_breakers[active_source_name].record_success()
                
                yield make_sse(IngestionProgressEvent(
                    stage="FETCHING",
                    level="SUCCESS",
                    source=active_source_name,
                    message=f"Extracted {len(raw_items)} raw job entries successfully from {active_source_name}.",
                    metadata={"count": len(raw_items)}
                ))
                fetch_success = True
                break

            except Exception as e:
                err_msg = str(e)
                if active_source_name in circuit_breakers:
                    circuit_breakers[active_source_name].record_failure(err_msg)

                yield make_sse(IngestionProgressEvent(
                    stage="FETCH_FAILED",
                    level="WARNING",
                    source=active_source_name,
                    message=f"Fetch failed on {active_source_name}: {err_msg}",
                    metadata={"error": err_msg, "attempt": attempt + 1}
                ))

                # If primary failed and we haven't tried fallback yet, switch to fallback!
                if active_source_name == primary_source_name and not fallback_used:
                    yield make_sse(IngestionProgressEvent(
                        stage="FAILOVER",
                        level="WARNING",
                        source=primary_source_name,
                        message=f"Primary source {primary_source_name} failed. Tripping failover router to {fallback_source_name}...",
                        metadata={"new_source": fallback_source_name}
                    ))
                    active_source_name = fallback_source_name
                    fallback_used = True
                    # continue loop to try fallback immediately
                    continue

        if not fetch_success or not raw_items:
            completed_at = datetime.utcnow().isoformat()
            record_run(
                run_id=run_id, primary_source=primary_source_name, status="FAILED",
                started_at=started_at, completed_at=completed_at, fallback_used=fallback_used,
                error_message="All ingestion sources exhausted or blocked."
            )
            yield make_sse(IngestionProgressEvent(
                stage="PIPELINE_END",
                level="ERROR",
                message="Pipeline halted: All data sources failed or triggered rate limits.",
                metadata={"status": "FAILED"}
            ))
            return

        # Step 3: Normalization & Schema Drift Checking
        yield make_sse(IngestionProgressEvent(
            stage="NORMALIZING",
            level="INFO",
            source=active_source_name,
            message="Sanitizing HTML markup and validating schemas against Pydantic model contracts...",
        ))

        normalized_jobs, drift_report = DataNormalizer.process_batch(raw_items, active_source_name)

        if drift_report:
            yield make_sse(IngestionProgressEvent(
                stage="DRIFT_DETECTED",
                level="WARNING",
                source=active_source_name,
                message=f"Schema drift detected! Drift score: {drift_report.drift_score:.2f}. Missing fields: {', '.join(drift_report.missing_fields)}",
                metadata=drift_report.model_dump()
            ))

        # Step 4: Storage in SQLite with deduplication
        yield make_sse(IngestionProgressEvent(
            stage="PERSISTING",
            level="INFO",
            message=f"Writing {len(normalized_jobs)} canonical records to SQLite database with hash deduplication...",
        ))

        db_result = save_jobs(normalized_jobs)
        completed_at = datetime.utcnow().isoformat()

        record_run(
            run_id=run_id,
            primary_source=primary_source_name,
            status="SUCCESS",
            started_at=started_at,
            completed_at=completed_at,
            fallback_used=fallback_used,
            items_extracted=len(raw_items),
            items_saved=db_result["inserted"],
            items_skipped_dup=db_result["duplicates"]
        )

        yield make_sse(IngestionProgressEvent(
            stage="PIPELINE_COMPLETE",
            level="SUCCESS",
            message=f"Ingestion run completed! Saved: {db_result['inserted']} new jobs, Skipped duplicates: {db_result['duplicates']}, Source: {active_source_name}",
            metadata={
                "run_id": run_id,
                "inserted": db_result["inserted"],
                "duplicates": db_result["duplicates"],
                "fallback_used": fallback_used,
                "active_source": active_source_name
            }
        ))
