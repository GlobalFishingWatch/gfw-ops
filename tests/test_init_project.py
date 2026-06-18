import sys
import pytest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import init_project  # noqa


def test_main_renames_project(tmp_path):
    # Setup
    old_name = "python-app-template"
    new_name = "my_project"

    old_dir_name = old_name.replace("-", "_")
    new_dir_name = new_name.replace("-", "_")

    # Create a fake source directory and package
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    old_pkg_dir = src_dir / old_dir_name
    old_pkg_dir.mkdir()

    # Create a version file
    version_file = old_pkg_dir / "version.py"
    version_file.write_text("VERSION = '0.1.0'\n# python-app-template\n")

    # Create a single additional file to update
    readme = tmp_path / "README.md"
    readme.write_text("Welcome to python-app-template!\n")

    # Run main()
    args = ["init_project.py", new_name]
    files_to_update = (str(readme),)
    init_project.main(
        args=args,
        old_name=old_name,
        files_to_update=files_to_update,
        src_dir=src_dir
    )

    # Check that file was updated
    updated_readme = readme.read_text()
    print(updated_readme)
    assert "my-project" in updated_readme
    assert "python-app-template" not in updated_readme

    # Check that version file was updated
    new_pkg_dir = src_dir / new_dir_name
    new_version_file = new_pkg_dir / "version.py"
    assert new_pkg_dir.exists()
    assert new_version_file.exists()
    assert "my-project" in new_version_file.read_text()

    # Old package dir should be gone
    assert not old_pkg_dir.exists()


def test_name_str():
    name = init_project.Name("my_project-name")
    expected = f"{name.with_dashes} / {name.with_underscores}"
    assert str(name) == expected
    # Specifically check the expected strings too
    assert name.with_dashes == "my-project-name"
    assert name.with_underscores == "my_project_name"


def test_rename_package_dir_old_path_missing(tmp_path, capsys):
    src_dir = tmp_path
    old_name = "oldpkg"
    new_name = "newpkg"

    # old_path does NOT exist (do nothing)
    # new_path also doesn't exist

    init_project._rename_package_dir(src_dir, old_name, new_name)

    captured = capsys.readouterr()
    expected_warning = f"⚠️ Warning: {src_dir / old_name} does not exist."
    assert expected_warning in captured.out


def test_rename_package_dir_new_path_exists(tmp_path, capsys):
    src_dir = tmp_path
    old_name = "oldpkg"
    new_name = "newpkg"

    old_path = src_dir / old_name
    new_path = src_dir / new_name

    # Create old_path directory so it exists
    old_path.mkdir()
    # Create new_path directory so it exists and triggers the warning
    new_path.mkdir()

    init_project._rename_package_dir(src_dir, old_name, new_name)

    captured = capsys.readouterr()
    expected_warning = f"⚠️ Warning: {new_path} already exists."
    assert expected_warning in captured.out


def test_update_project_name_skips_missing_file(tmp_path, capsys):
    old_name = init_project.Name("old-project")
    new_name = init_project.Name("new-project")

    # File that does not exist
    missing_file = tmp_path / "missing_file.txt"
    # Don't create the file, so it is missing

    init_project.update_project_name(
        old_name=old_name,
        new_name=new_name,
        src_dir=tmp_path,
        files_to_update=(str(missing_file),)
    )

    captured = capsys.readouterr()
    assert f"⏭️ Skipped (not found): {missing_file}" in captured.out


def test_main_usage_message_on_bad_args(capsys):
    bad_args = ["script_name.py"]  # only 1 argument instead of 2

    with pytest.raises(SystemExit) as exc_info:
        init_project.main(args=bad_args)

    captured = capsys.readouterr()
    example = init_project.Name("my-project-name")
    expected_usage = (
        f"Usage: python {bad_args[0]} {example.with_dashes} "
        f"OR {example.with_underscores}"
    )

    assert expected_usage in captured.out
    assert exc_info.value.code == 1
