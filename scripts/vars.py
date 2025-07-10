import os

import toml


def get_package_name() -> str:
    with open("pyproject.toml", "r") as file:
        pyproject_data = toml.load(file)
    pkg_name = pyproject_data["tool"]["poetry"]["name"]
    assert isinstance(pkg_name, str)
    return pkg_name


def get_python_version() -> str:
    with open("pyproject.toml", "r") as file:
        pyproject_data = toml.load(file)
    python_version = pyproject_data["tool"]["poetry"]["dependencies"]["python"]
    assert isinstance(python_version, str)
    print(python_version)  # required for publish-version GH action
    return python_version


def get_git_branch() -> str:
    return os.popen("git branch --show-current").read().strip()
