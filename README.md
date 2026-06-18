<h1 align="center" style="border-bottom: none;">  python-app-template </h1>

<p align="center">
  <a href="https://github.com/GlobalFishingWatch/python-app-template/actions/workflows/main.yaml" >
    <img src="https://github.com/GlobalFishingWatch/python-app-template/actions/workflows/main.yaml/badge.svg"/>
  </a>
  <a href="https://codecov.io/gh/GlobalFishingWatch/python-app-template" >
    <img src="https://codecov.io/gh/GlobalFishingWatch/python-app-template/graph/badge.svg?token=uZTb6EphP8"/>
  </a>
  <a>
    <img alt="Python versions" src="https://img.shields.io/badge/python-3.12%20%7C%203.13-blue">
  </a>
  <a>
    <img alt="Last release" src="https://img.shields.io/github/v/release/GlobalFishingWatch/python-app-template">
  </a>
</p>

A template for Python applications.

**Features**:
* :white_check_mark: Standard Python project structure & packaging.
* :white_check_mark: Dependency management with [pip-tools].
* :white_check_mark: Tools for quality checks: documentation, [PEP8], typehints, codespell.
* :white_check_mark: **[Optional]** pre-commit hooks to enforce automatic quality checks.
* :white_check_mark: Dockerization with focus in image size optimization.
* :white_check_mark: Continuous Integration (CI) workflows (GitHub Actions).
* :white_check_mark: Continuous Deployment (CI) workflows (Google Cloud Build).
* :white_check_mark: Makefile with shortcuts to increase development speed.
* :white_check_mark: README badges with project information.
* :white_check_mark: Development workflow documentation.
* :white_check_mark: Support for [Apache Beam] integrated pipelines.


[Apache Beam]: https://beam.apache.org
[branch protection rules]: https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/managing-a-branch-protection-rule
[codecov]: https://about.codecov.io
[docker compose]: https://docs.docker.com/compose/install/linux/
[Google BigQuery]: https://cloud.google.com/bigquery
[Google Dataflow]: https://cloud.google.com/products/dataflow?hl=en
[Markdown alerts]: https://docs.github.com/en/get-started/writing-on-github/getting-started-with-writing-and-formatting-on-github/basic-writing-and-formatting-syntax#alerts
[mermaid diagram]: https://docs.github.com/en/get-started/writing-on-github/working-with-advanced-formatting/creating-diagrams
[PEP8]: https://peps.python.org/pep-0008/
[pip-tools]: https://pip-tools.readthedocs.io/en/stable/
[pre-commit]: https://pre-commit.com
[pytest]: https://docs.pytest.org/en/stable/
[used by pytest-cov]: https://pytest-cov.readthedocs.io/en/latest/config.html

[cli.py]: src/python_app_template/cli.py
[docs/contributing]: docs/contributing
[examples]: examples/
 

[.github/]: .github/
[docs/]: docs/
[notebooks/]: notebooks/
[src/python_app_template/]: src/python_app_template 
[src/python_app_template/assets/]: src/python_app_template/assets/
[tests/]: tests/

[.dockerignore]: .dockerignore
[.flake8]: .flake8
[.gitignore]: .gitignore
[.pre-commit-config.yaml]: .pre-commit-config.yaml
[activate-venv.sh]: activate-venv.sh
[cloudbuild.yaml]: cloudbuild.yaml
[codecov.yml]: codecov.yml
[CONTRIBUTING.md]: CONTRIBUTING.md
[docker-compose.yml]: docker-compose.yml
[Dockerfile]: Dockerfile
[Git Workflow]: CONTRIBUTING.md#git-workflow
[init_project.py]: init_project.py
[LICENSE]: LICENSE
[Makefile]: Makefile
[MANIFEST.in]: MANIFEST.in
[pyproject.toml]: pyproject.toml
[pytest.ini]: pytest.ini
[README.md]: README.md
[requirements.txt]: requirements.txt
[requirements-test.txt]: requirements-test.txt
[setup.py]: setup.py


## Introduction

<div align="justify">

_Write the motivation for this application here._
_When applicable, include citations for relevant articles or resources for further reading._

The motivation for this repository is to provide a robust,
well-documented template for Python applications,
including but not limited to GFW data pipelines.

Goals of this template include:
* Reducing the overhead of creating a new Dockerized Python application.
* Minimizing the effort required to transition prototypes to production.
* Establishing consistent development standards across projects.

## Usage 

_Write the relevant documentation on how to use this application here.
Ideally, organize it into sections_.

_Assume in this section that a package has been published in PYPI
or a Docker image to the registry._

_Use [Markdown alerts] to highlight important information._

</div>

Example:
> [!CAUTION]
  Using HTML sections breaks the rendering of [Markdown alerts] alerts.
  To avoid this, place them outside `<div></div>` sections, for example.

### Minimum Requirements

This template can be used at various stages of development:
**Proof of Concept**, **Prototype**, and **Production**.  
Each stage has its own minimum quality requirements
and incrementally builds upon the requirements of the previous stages.


1. 💡 **Proof of Concept**  
   - Update the project's name with `python init_project.py my-project-name`.
   - Declare required dependencies in [pyproject.toml].
     Use the `dev` extra for development-only dependencies.
   - Store data files (e.g., `.txt`, `.json`, `.csv`) in the `src/my_project_name/assets/` directory.
   - Add the following sections to the `README.md`:  
     - **Introduction**  
     - **Usage**

2. 🛠️ **Prototype**  
   - Set up [branch protection rules] for `main` branch (and `develop` if exists).
     - ☑️ Restrict deletions.
     - ☑️ Require a pull request before merging. 
       - ☑️ Require approvals.
       - ☑️ Require conversation resolution before merging.
     - ☑️ Require status checks to pass. Add GitHub actions to checks.
       - ☑️ Require branches to be up to date before merging.
     - ☑️ Block force pushes.
     - ☑️ Allowed merge methods: only Merge.
   - Select a [Git Workflow] to maintain consistent branching and collaboration practices.
   - Set up Google Cloud Build triggers to automatically publish the Docker image upon merges.
   - Re-compile `requirements.txt` for Docker using:
     ```
     make reqs
     ```
   - Update the `README.md` to include:  
     - **Output Schema**

3. 🚀 **Production**  
   - Install and configure pre-commit hooks. See [CONTRIBUTING.md].
   - Add unit tests to ensure code reliability.
   - Write thorough documentation:  
     - A complete `README.md`.
     - Docstrings for all **public** modules, classes, methods and functions.



### Repository Overview

_This section applies only to the template and provides an overview of the repository contents._

#### Directories

This is a brief summary of all the relevant directories of the repository.

<div  align="center">

| Directory                       | Description                                                                     |
| ------------------------------- | ------------------------------------------------------------------------------- |
|[.github/]                       | Configuration for GitHub actions.                                               |
|[docs/]                          | Markdown files with detailed documentation.                                     |
|[notebooks/]                     | All jupyter notebooks go here.                                                  |
|[src/python_app_template/]       | All source code go here.                                                        |
|[src/python_app_template/assets/]| All data files go here.                                                         |
|[tests/]                         | All tests go here.                                                              |

</div>

#### Files

This is a brief summary of all the relevant files of the repository.

<div  align="center">

| File                           | Description                                                                     |
| -------------------------------| ------------------------------------------------------------------------------- |
|[.dockerignore]                 | Lists files and folders that Docker should ignore when building an image.       |
|[.flake8]                       | Configuration for [PEP8] checker.                                               |
|[.gitignore]                    | List of files and directories to be ignored by git.                             |
|[.pre-commit-config.yaml]       | Configuration to automate software quality checks.                              |
|[activate-venv.sh]              | Simple shortcut to enter virtual environment.                                   |
|[cloudbuild.yaml]               | Configuration to build and publish docker images in Google Cloud.               |
|[codecov.yml]                   | Configuration for [codecov] GitHub integration.                                 |
|[CONTRIBUTING.md]               | A guide for contributing to the project.                                        |
|[docker-compose.yml]            | Configuration for [docker compose].                                             |
|[Dockerfile]                    | Instructions for building the Docker image.                                     |
|[init_project.py]               | Automates changing the project name across relevant files and directories.      |
|[LICENSE]                       | The software license.                                                           |
|[Makefile]                      | Set of commands to simplify development.                                        |
|[MANIFEST.in]                   | Set of patterns to include or exclude files from installed package.             |
|[pyproject.toml]                | Modern Python packaging configuration file.                                     |
|[requirements.txt]              | Full set of compiled production dependencies (pinned to specific versions).     |
|[requirements-test.txt]         | High level test dependencies needed to test the installed package.              |
|[README.md]                     | This file.                                                                      |
|[setup.py]                      | Legacy Python packaging config file, kept for compatibility with [Apache Beam]. |

</div>

<div  align="justify">

### Using the CLI

_Write instructions on how to use the CLI of the application here._

#### Config file example

_**Optional**_.
_Provide an example of an input configuration file._

## Output schema.

_**Optional**_.
_Discuss any relevant details about the schema.
Provide a link to the schema definition._

## Data persistence pattern

_**Optional**_.
_Explain the data persistence pattern used in this application._

## How to Contribute

Please read the guidelines in [CONTRIBUTING.md].

## Implementation details

_**Optional**_.
_This section is for describing implementation details, primarily for developers._

### Most relevant modules

_**Optional**_.
_Use this section to describe the most important modules of your application._

Example:
<div align="center">

| Module | Description |
| --- | --- |
| [cli.py]     | Defines the application CLI. |

</div>

### Flow chart

_**Optional**_.
_Use this section to display a [mermaid diagram] that explains the implementation._

## References

_**Optional**_.
_Provide any relevant references to support the explanations in this README.
Here we provide some examples._


<a id="1">[1]</a> Welch H., Clavelle T., White T. D., Cimino M. A., Van Osdel J., Hochberg T., et al. (2022). Hot spots of unseen fishing vessels. Sci. Adv. 8 (44), eabq2109. doi: 10.1126/sciadv.abq2109

<a id="1">[2]</a> J. H. Ford, B. Bergseth, C. Wilcox, Chasing the fish oil—Do bunker vessels hold the key to fisheries crime networks? Front. Mar. Sci. 5, 267 (2018).