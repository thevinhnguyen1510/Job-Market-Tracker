"""Structured fields extracted from a job description."""

from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field, model_validator


class EnglishLevel(str, Enum):
    """Accepted categories for English requirements in job postings."""

    NONE = "Not mentioned"
    READING = "Read/Write basic documentations"
    COMMUNICATION = "Communicate with clients"
    CERTIFICATE = "Required certificate (IELTS/TOEIC)"

class JobExtraction(BaseModel):
    """Validate the structured fields returned by job description extraction."""

    min_years_of_experience: int = Field(
        ...,
        description="Extract MINIMUM practical years. '1-3 years' -> 1. 'Fresher/Intern/None' -> 0."
    )
    core_tech_stack: list[str] = Field(
        ...,
        max_length=5,
        description="Max 5 hard tech skills only. Ex: ['Python', 'SQL', 'AWS', 'PostgreSQL']."
    )
    english_requirement: EnglishLevel = Field(
        ...,
        description="Classify English requirement from JD."
    )
    job_role: Literal[
        "Backend", "Frontend", "Fullstack", "Mobile", "Data Engineer",
        "Data Scientist", "Data/Business Analyst", "Product Owner/Manager",
        "QA/QC/Tester", "DevOps/Cloud", "System Admin", "UI/UX Designer",
        "Security", "Scrum Master", "AI/Machine Learning", "Unknown"
    ] = Field(..., description="Standard job role. General Software Engineer/Developer roles map to 'Backend' unless frontend/mobile is specified.")

    job_level: Literal["Intern", "Fresher", "Junior", "Middle", "Senior", "Manager", "Director", "Unknown"] = Field(
        ..., description="Standard seniority level."
    )

    @model_validator(mode='after')
    def infer_job_level(self) -> 'JobExtraction':
        """Infer seniority from required experience only when the level is unknown."""
        if self.job_level == "Unknown":
            years = self.min_years_of_experience
            if years == 0: self.job_level = "Fresher"
            elif 1 <= years <= 2: self.job_level = "Junior"
            elif 3 <= years <= 4: self.job_level = "Middle"
            elif years >= 5: self.job_level = "Senior"
        return self
