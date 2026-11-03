<h1 align="center" style="border-bottom: none;">  gfw-ops </h1>

<p align="center">
  <a href="https://github.com/GlobalFishingWatch/gfw-ops/actions/workflows/main.yaml" >
    <img src="https://github.com/GlobalFishingWatch/gfw-ops/actions/workflows/main.yaml/badge.svg"/>
  </a>
  <a href="https://codecov.io/gh/GlobalFishingWatch/gfw-ops" >
    <img src="https://codecov.io/gh/GlobalFishingWatch/gfw-ops/graph/badge.svg?token=B4EHNI359E"/>
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
* :white_check_mark: `test-report` — posts a weekly report of how the test suites ran (success rate and average duration) to Slack.


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

### `test-report`

Posts a report of how the test suites ran over the past week to Slack:

```
📊 Weekly test report · 27 Oct – 2 Nov

Live monitoring (production)
• API (daily): 100% passed (5/5) · avg 10m
• Frontend (daily): 0% passed (0/4) · avg 8m 🔴

CI checks (dev/staging)
• Frontend (on every PR): 67% passed (22/33) · avg 5m
```

Runs that were skipped or cancelled, and runs still in progress, are not counted.
A suite is flagged 🔴 when fewer than half of its runs passed.
The window is made of whole UTC days ending today, so running it on Monday reports the previous Monday to Sunday.

```shell
gfw-ops test-report --dry-run                    # print the report instead of posting it
gfw-ops test-report --slack-channel C0123456789  # post it
```

Credentials come from the environment:

| Source | Credentials |
| --- | --- |
| Cloud Build (API tests) | Application Default Credentials (needs permission to list builds in `gfw-int-infrastructure`) |
| GitHub Actions (frontend) | `GITHUB_TOKEN` with read access to Actions on `GlobalFishingWatch/frontend` |
| Slack | `SLACK_TOKEN` of a Slack app with `chat:write`, which must be a member of the channel |

If the runs of a suite cannot be read, the report still goes out with "data unavailable" on that suite and the command exits with an error.

The suites are listed in [suites.py](src/gfw/ops/pipelines/test_report/suites.py); to add one, append a `Suite` with the source its runs come from.

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
