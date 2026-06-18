"""Renames a Python project by replacing the template name in files and directories."""
import sys
import pathlib

OLD_NAME = "python-app-template"

FILES_TO_UPDATE = (
    "cloudbuild.yaml",
    "CONTRIBUTING.md",
    "docker-compose.yml",
    "Makefile",
    "pyproject.toml",
    "README.md",
    "requirements.txt",
    "tests/test_assets.py",
    "tests/test_version.py",
    "tests/cli/test_main.py",
)

VERSION_FILE_TPL = "{src_dir}/{old_name}/version.py"

SOURCE_DIR = pathlib.Path("src")


class Name:
    """Encapsulates a project name with methods to convert between dash and underscore styles."""

    def __init__(self, raw: str):
        """Initialize Name with a raw string.

        Strips leading/trailing whitespace and replaces spaces with dashes.
        """
        self._raw = raw.strip().replace(" ", "-")

    @property
    def with_dashes(self) -> str:
        """Return the project name with dashes instead of underscores."""
        return self._raw.replace("_", "-")

    @property
    def with_underscores(self) -> str:
        """Returns the project name with underscores instead of dashes."""
        return self._raw.replace("-", "_")

    def __str__(self):
        """Returns a string representation showing both dash and underscore versions."""
        return f"{self.with_dashes} / {self.with_underscores}"


def _replace_in_file(filepath: pathlib.Path, old_new_pairs: list[tuple[str, str]]) -> None:
    content = filepath.read_text(encoding="utf-8")
    for old, new in old_new_pairs:
        content = content.replace(old, new)

    filepath.write_text(content, encoding="utf-8")


def _rename_package_dir(src_dir: pathlib.Path, old_name: str, new_name: str) -> None:
    old_path = src_dir / old_name
    new_path = src_dir / new_name

    if not old_path.exists():
        print(f"⚠️ Warning: {old_path} does not exist.")
        return

    if new_path.exists():
        print(f"⚠️ Warning: {new_path} already exists.")
        return

    old_path.rename(new_path)
    print(f"📁 Renamed package dir: {old_path} → {new_path}")


def update_project_name(
    old_name: Name,
    new_name: Name,
    src_dir: pathlib.Path,
    files_to_update: tuple[str, ...] = (),
) -> None:
    """Updates the project name in all relevant files and rename the source directory."""
    replacements = [
        (old_name.with_dashes, new_name.with_dashes),
        (old_name.with_underscores, new_name.with_underscores),
    ]

    print("🔁 Replacing names:")
    for o, n in replacements:
        print(f"  {o} → {n}")

    for file in files_to_update:
        path = pathlib.Path(file)
        if path.exists():
            _replace_in_file(path, replacements)
            print(f"✅ Updated: {file}")
        else:
            print(f"⏭️ Skipped (not found): {file}")

    _rename_package_dir(src_dir, old_name.with_underscores, new_name.with_underscores)
    print("\n✅ Done!")
    print(f"🎉 Project renamed to: {new_name.with_dashes}.")


def main(
    args: list[str] = sys.argv,
    old_name: str = OLD_NAME,
    files_to_update: tuple[str, ...] = FILES_TO_UPDATE,
    src_dir: pathlib.Path = SOURCE_DIR
):
    """Entry point for the script.

    Expects a single command-line argument: the new project name (with dashes or underscores).
    """
    example = Name("my-project-name")

    if len(args) != 2:
        print(f"Usage: python {args[0]} {example.with_dashes} OR {example.with_underscores}")
        sys.exit(1)

    old_name = Name(old_name)
    new_name = Name(args[1])

    files_to_update = list(files_to_update)
    version_file = VERSION_FILE_TPL.format(src_dir=src_dir, old_name=old_name.with_underscores)
    files_to_update.append(version_file)

    update_project_name(
        old_name=old_name,
        new_name=new_name,
        files_to_update=tuple(files_to_update),
        src_dir=src_dir
    )


if __name__ == "__main__":
    main()
