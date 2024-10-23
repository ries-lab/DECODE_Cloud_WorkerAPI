import os

import docker
import toml


def _get_client() -> docker.DockerClient:
    return docker.from_env()


def _get_package_name() -> str:
    with open("pyproject.toml", "r") as file:
        pyproject_data = toml.load(file)
    return pyproject_data["tool"]["poetry"]["name"]


def _get_python_version() -> str:
    with open("pyproject.toml", "r") as file:
        pyproject_data = toml.load(file)
    return pyproject_data["tool"]["poetry"]["dependencies"]["python"]


def build() -> None:
    """
    Builds a Docker image for the current branch.
    """
    git_branch = os.popen("git branch --show-current").read().strip()
    client = _get_client()
    client.images.build(
        path=os.path.join(os.path.dirname(__file__), ".."),
        tag=f"{_get_package_name()}:{git_branch}",
        nocache=True,
        rm=True,
        pull=True,
        buildargs={"PYTHON_VERSION": _get_python_version()},
    )


def cleanup() -> None:
    """
    Removes all Docker images for this package.
    """
    client = _get_client()
    for image in client.images.list(name=_get_package_name()):
        client.images.remove(image.id, force=True)
    client.images.prune(filters={"dangling": True})
