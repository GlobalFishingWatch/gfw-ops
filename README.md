<h1 align="center" style="border-bottom: none;">  gfw-ops </h1>

<p align="center">
  <a href="https://github.com/GlobalFishingWatch/gfw-ops/actions/workflows/main.yaml" >
    <img src="https://github.com/GlobalFishingWatch/gfw-ops/actions/workflows/main.yaml/badge.svg"/>
  </a>
  <a href="https://codecov.io/gh/GlobalFishingWatch/gfw-ops" >
    <img src="https://codecov.io/gh/GlobalFishingWatch/gfw-ops/graph/badge.svg?token=uZTb6EphP8"/>
  </a>
  <a>
    <img alt="Python versions" src="https://img.shields.io/badge/python-3.13%20%7C%203.14-blue">
  </a>
  <a>
    <img alt="Last release" src="https://img.shields.io/github/v/release/GlobalFishingWatch/gfw-ops">
  </a>
</p>

Reusable operational data engineering tools for GFW projects.

**Features**:
* :white_check_mark: `sharded-to-partitioned` — migrates BigQuery sharded tables to partitioned tables.
* :white_check_mark: `bq-to-parquet` — exports BigQuery tables to Parquet files on GCS.


[cli.py]: src/gfw_ops/cli.py
[CONTRIBUTING.md]: CONTRIBUTING.md


## Introduction

<div align="justify">

GFW maintains two shared Python repositories:

- [`gfw-common`](https://github.com/GlobalFishingWatch/gfw-common) — a pure library of reusable components
(Beam transforms, BigQuery helpers, CLI framework, etc.) imported by pipeline repos.
- `pipe-*` repos — individual pipeline applications tied to a specific data domain.

`gfw-ops` fills the gap between these two:
it is the home for **operational data engineering tools that are generic across all projects**
but are applications, not library code.
Tools here run as config-driven jobs — triggered from Airflow,
Cloud Build, or the command line
— without requiring changes to any pipeline-specific repository.

## Usage 

### Using the CLI

_Write instructions on how to use the CLI of the application here._

#### Config file example

_**Optional**_.
_Provide an example of an input configuration file._

## How to Contribute

Please read the guidelines in [CONTRIBUTING.md].

## Implementation details

_**Optional**_.
_This section is for describing implementation details, primarily for developers._

TBC.

### Most relevant modules

_**Optional**_.
_Use this section to describe the most important modules of your application._

Example:
<div align="center">

| Module | Description |
| --- | --- |
| [cli.py]     | Defines the application CLI. |

</div>
