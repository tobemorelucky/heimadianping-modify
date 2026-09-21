"""Metadata registry and deterministic Incident-to-Skill matching."""

from __future__ import annotations

from pathlib import Path

from runtime.models import IncidentTask
from skills.loader import SkillLoader
from skills.models import LoadedSkill, SkillMetadata


class SkillRegistry:
    """Discover skill metadata without eagerly loading instruction bodies."""

    def __init__(
        self,
        skills_root: Path | str | None = None,
        *,
        loader: SkillLoader | None = None,
    ) -> None:
        root = Path(skills_root or Path(__file__).resolve().parent).resolve()
        self.skills_root = root
        self.loader = loader or SkillLoader(root)
        self._metadata: dict[str, SkillMetadata] | None = None

    def discover_skills(self, *, refresh: bool = False) -> tuple[SkillMetadata, ...]:
        """Scan immediate skill directories and cache metadata-only records."""

        if self._metadata is None or refresh:
            discovered: dict[str, SkillMetadata] = {}
            for path in sorted(self.skills_root.glob("*/SKILL.md")):
                metadata = self.loader.load_metadata(path)
                if metadata.skill_id in discovered:
                    raise ValueError(f"duplicate skill_id: {metadata.skill_id}")
                discovered[metadata.skill_id] = metadata
            self._metadata = discovered
        return tuple(self._metadata[key] for key in sorted(self._metadata))

    def match_skills(self, incident: IncidentTask) -> tuple[SkillMetadata, ...]:
        """Rank skills by deterministic trigger matches against Incident fields."""

        incident_text = self._normalize(
            " ".join(
                [incident.title, incident.description, *incident.affected_components]
            )
        )
        scored: list[tuple[int, int, int, str, SkillMetadata]] = []
        for metadata in self.discover_skills():
            matches = [
                trigger
                for trigger in metadata.trigger_conditions
                if trigger in incident_text
            ]
            if not matches:
                continue
            specific_category = 0 if metadata.category == "general" else 1
            scored.append(
                (
                    len(matches),
                    specific_category,
                    max(len(trigger) for trigger in matches),
                    metadata.skill_id,
                    metadata,
                )
            )
        scored.sort(key=lambda item: (-item[0], -item[1], -item[2], item[3]))
        return tuple(item[-1] for item in scored)

    def get_skill(self, skill_id: str) -> SkillMetadata:
        """Return one discovery record without loading its instructions."""

        if self._metadata is None:
            self.discover_skills()
        assert self._metadata is not None
        try:
            return self._metadata[skill_id]
        except KeyError as exc:
            raise KeyError(f"unknown skill_id: {skill_id}") from exc

    def load_skill(self, skill_id: str) -> LoadedSkill:
        """Materialize a selected skill through the progressive loader."""

        return self.loader.load(self.get_skill(skill_id))

    @staticmethod
    def _normalize(value: str) -> str:
        return " ".join(value.casefold().split())
