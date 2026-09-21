"""Progressive SKILL.md loader with path confinement."""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

from skills.models import LoadedSkill, SkillMetadata, SkillReference


FRONT_MATTER_DELIMITER = "+++"


class SkillLoader:
    """Read front matter for discovery and full content only after selection."""

    def __init__(self, skills_root: Path | str | None = None) -> None:
        self.skills_root = Path(skills_root or Path(__file__).resolve().parent).resolve()

    def load_metadata(self, skill_path: Path | str) -> SkillMetadata:
        """Read only the TOML front matter and stop before the Markdown body."""

        path = self._validated_skill_path(skill_path)
        metadata_lines: list[str] = []
        with path.open("r", encoding="utf-8") as stream:
            if stream.readline().strip() != FRONT_MATTER_DELIMITER:
                raise ValueError(f"SKILL.md is missing TOML front matter: {path}")
            for line in stream:
                if line.strip() == FRONT_MATTER_DELIMITER:
                    break
                metadata_lines.append(line)
            else:
                raise ValueError(f"SKILL.md front matter is not closed: {path}")

        try:
            raw: dict[str, Any] = tomllib.loads("".join(metadata_lines))
        except tomllib.TOMLDecodeError as exc:
            raise ValueError(f"invalid SKILL.md metadata in {path}: {exc}") from exc
        raw["path"] = path
        return SkillMetadata.model_validate(raw)

    def load(self, metadata: SkillMetadata) -> LoadedSkill:
        """Load a selected skill's full instructions and local references."""

        path = self._validated_skill_path(metadata.path)
        current_metadata = self.load_metadata(path)
        if current_metadata.skill_id != metadata.skill_id:
            raise ValueError("skill metadata changed between discovery and loading")

        text = path.read_text(encoding="utf-8")
        instructions = self._markdown_body(text, path)
        references_dir = path.parent / "references"
        references: list[SkillReference] = []
        if references_dir.is_dir():
            for reference_path in sorted(references_dir.glob("*.md")):
                resolved_reference = reference_path.resolve()
                self._require_within_root(resolved_reference)
                references.append(
                    SkillReference(
                        name=resolved_reference.stem,
                        path=resolved_reference,
                        content=resolved_reference.read_text(encoding="utf-8").strip(),
                    )
                )
        return LoadedSkill(
            metadata=current_metadata,
            instructions=instructions,
            references=tuple(references),
        )

    def _validated_skill_path(self, skill_path: Path | str) -> Path:
        path = Path(skill_path).resolve()
        self._require_within_root(path)
        if path.name != "SKILL.md" or not path.is_file():
            raise ValueError(f"skill path must identify an existing SKILL.md: {path}")
        return path

    def _require_within_root(self, path: Path) -> None:
        try:
            path.relative_to(self.skills_root)
        except ValueError as exc:
            raise ValueError(f"skill path is outside the configured root: {path}") from exc

    @staticmethod
    def _markdown_body(text: str, path: Path) -> str:
        lines = text.splitlines()
        if not lines or lines[0].strip() != FRONT_MATTER_DELIMITER:
            raise ValueError(f"SKILL.md is missing TOML front matter: {path}")
        closing = next(
            (index for index, line in enumerate(lines[1:], start=1) if line.strip() == FRONT_MATTER_DELIMITER),
            None,
        )
        if closing is None:
            raise ValueError(f"SKILL.md front matter is not closed: {path}")
        body = "\n".join(lines[closing + 1 :]).strip()
        if not body:
            raise ValueError(f"SKILL.md contains no instructions: {path}")
        return body
