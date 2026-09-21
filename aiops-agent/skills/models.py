"""Validated contracts for metadata-only and fully loaded Agent skills."""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, field_validator


class SkillMetadata(BaseModel):
    """Small discovery record read from a SKILL.md front matter block."""

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    skill_id: str = Field(
        min_length=3,
        max_length=80,
        pattern=r"^[a-z][a-z0-9-]*$",
    )
    name: str = Field(min_length=1, max_length=120)
    description: str = Field(min_length=1, max_length=500)
    category: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[a-z][a-z0-9_-]*$",
    )
    trigger_conditions: tuple[str, ...] = Field(min_length=1, max_length=32)
    version: str = Field(min_length=1, max_length=32, pattern=r"^[0-9]+\.[0-9]+$")
    path: Path

    @field_validator("trigger_conditions")
    @classmethod
    def normalize_triggers(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        normalized: list[str] = []
        for value in values:
            trigger = " ".join(value.casefold().split())
            if not trigger:
                raise ValueError("skill trigger conditions must not be blank")
            if trigger not in normalized:
                normalized.append(trigger)
        return tuple(normalized)


class SkillReference(BaseModel):
    """One reference document loaded only after its parent skill is selected."""

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    name: str = Field(min_length=1, max_length=120)
    path: Path
    content: str = Field(min_length=1, max_length=20_000)


class SkillContext(BaseModel):
    """Bounded, serializable guidance exposed to the Planner."""

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    skill_id: str = Field(min_length=3, max_length=80)
    name: str = Field(min_length=1, max_length=120)
    description: str = Field(min_length=1, max_length=500)
    category: str = Field(min_length=1, max_length=64)
    version: str = Field(min_length=1, max_length=32)
    instructions: str = Field(min_length=1, max_length=20_000)
    references: tuple[SkillReference, ...] = Field(default_factory=tuple, max_length=16)


class LoadedSkill(BaseModel):
    """Full skill body and references materialized on demand."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    metadata: SkillMetadata
    instructions: str = Field(min_length=1, max_length=20_000)
    references: tuple[SkillReference, ...] = Field(default_factory=tuple, max_length=16)

    def planner_context(self) -> SkillContext:
        """Return only the guidance contract that may be sent to the Planner."""

        return SkillContext(
            skill_id=self.metadata.skill_id,
            name=self.metadata.name,
            description=self.metadata.description,
            category=self.metadata.category,
            version=self.metadata.version,
            instructions=self.instructions,
            references=self.references,
        )
