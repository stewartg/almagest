import subprocess
from pathlib import Path

import tomlkit

"""
This script is used after creating a Python virtual environment and installing required
libraries using a pyproject.toml file that originally contains some or no library version
numbers.The script updates the pyproject.toml file by pinning version numbers from the
currently installed packages. The script also attempts to preserve exising spacing and
layout of the original file as much as possible.
"""


def get_installed_packages() -> list[str]:
    """Retrieves a list of installed packages in the format 'package_name==version'.

    :return: A list of strings representing installed packages and their versions.
    """
    result = subprocess.run(["pip", "freeze"], stdout=subprocess.PIPE)  # noqa: S607, S603
    return [line.decode().strip() for line in result.stdout.splitlines()]


def update_pyproject_toml(file_path: str, all_packages: list[str]) -> None:
    """Updates the pyproject.toml file with the installed package versions.

    :param file_path: The path to the pyproject.toml file.
    :param all_packages: A list of strings representing installed packages and their versions.
    """
    # Create a dictionary from the installed packages for quick lookup
    package_versions = {pkg.split("==")[0].lower(): pkg.split("==")[1] for pkg in all_packages if "==" in pkg}

    with Path(file_path).open("r") as fin:
        data = tomlkit.load(fin)

    proj_dep = data["project"]["dependencies"]
    proj_dep.sort()

    # Update project.dependencies
    for i in range(len(proj_dep)):
        package_name = proj_dep[i].split("==")[0] if "==" in proj_dep[i] else proj_dep[i]
        version = package_versions.get(package_name.lower())
        proj_dep[i] = f"{package_name}=={version}" if version else proj_dep[i]

    # Update project.optional-dependencies
    optional_deps = data["project"]["optional-dependencies"]
    for _dep_group, deps in optional_deps.items():
        deps.sort()
        for i in range(len(deps)):
            package_name = deps[i].split("==")[0] if "==" in deps[i] else deps[i]
            version = package_versions.get(package_name.lower())
            deps[i] = f"{package_name}=={version}" if version else deps[i]

    with Path(file_path).open("w") as fout:
        fout.write(tomlkit.dumps(data))


if __name__ == "__main__":
    all_packages = get_installed_packages()
    update_pyproject_toml("pyproject.toml", all_packages)
