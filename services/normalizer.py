"""
Data Normalization & Schema Validation Layer.
Uses BeautifulSoup4 for HTML sanitization and Pydantic for schema verification.
Detects markup/schema drift without crashing the pipeline.
"""

from bs4 import BeautifulSoup
import re
from typing import List, Tuple, Optional
from models import RawJobItem, NormalizedJob, SchemaDriftReport
from database import record_schema_drift


class DataNormalizer:
    """Cleans, standardizes, and validates extracted job payloads."""

    @staticmethod
    def clean_html(raw_html: Optional[str], max_len: int = 280) -> str:
        """Strips HTML tags, scripts, and extra whitespace, generating a clean text summary."""
        if not raw_html:
            return "No description provided."
        
        soup = BeautifulSoup(raw_html, "html.parser")
        # Remove script and style elements
        for element in soup(["script", "style", "nav", "footer", "iframe"]):
            element.decompose()
            
        text = soup.get_text(separator=" ", strip=True)
        # Normalize whitespace
        text = re.sub(r"\s+", " ", text).strip()
        
        if len(text) > max_len:
            return text[:max_len].rsplit(" ", 1)[0] + "..."
        return text

    @staticmethod
    def normalize_salary(raw_salary: Optional[str]) -> Optional[str]:
        """Sanitizes compensation strings."""
        if not raw_salary:
            return None
        cleaned = raw_salary.strip()
        if cleaned.lower() in ["n/a", "none", "null", ""]:
            return None
        return cleaned

    @classmethod
    def process_batch(cls, raw_items: List[RawJobItem], source_name: str) -> Tuple[List[NormalizedJob], Optional[SchemaDriftReport]]:
        """
        Normalizes a batch of raw items.
        Returns:
            (valid_normalized_jobs, drift_report_or_none)
        """
        valid_jobs: List[NormalizedJob] = []
        failed_count = 0
        missing_fields_set = set()
        sample_error_snippet = ""

        for item in raw_items:
            # Check mandatory fields
            missing = []
            if not item.raw_title or len(item.raw_title.strip()) < 2:
                missing.append("title")
            if not item.raw_company or len(item.raw_company.strip()) < 1:
                missing.append("company")
            if not item.raw_url:
                missing.append("url")

            if missing:
                failed_count += 1
                missing_fields_set.update(missing)
                sample_error_snippet = f"Title: {item.raw_title} | Company: {item.raw_company} | RawDesc: {item.raw_description[:100] if item.raw_description else 'None'}"
                continue

            try:
                title = item.raw_title.strip()
                company = item.raw_company.strip()
                url = item.raw_url.strip()
                location = (item.raw_location or "Remote").strip()
                desc_snippet = cls.clean_html(item.raw_description)
                salary = cls.normalize_salary(item.raw_salary)
                tags = [t.strip() for t in (item.raw_tags or []) if t and len(t.strip()) > 1][:8]

                job_id = NormalizedJob.generate_id(source_name, title, company, url)
                
                normalized = NormalizedJob(
                    id=job_id,
                    source=source_name,
                    title=title,
                    company=company,
                    location=location,
                    description_snippet=desc_snippet,
                    url=url,
                    salary=salary,
                    tags=tags,
                    published_date=item.raw_published_at or "Recent"
                )
                valid_jobs.append(normalized)
            except Exception as e:
                failed_count += 1
                sample_error_snippet = f"Exception: {str(e)} on item {item.raw_title}"

        # If failures detected, record schema drift telemetry
        drift_report = None
        if failed_count > 0:
            total_items = len(raw_items) or 1
            drift_score = round(failed_count / total_items, 3)
            drift_report = SchemaDriftReport(
                source_name=source_name,
                missing_fields=list(missing_fields_set),
                unexpected_structure=(drift_score > 0.4),
                drift_score=drift_score,
                sample_payload_snippet=sample_error_snippet
            )
            record_schema_drift(drift_report)

        return valid_jobs, drift_report
