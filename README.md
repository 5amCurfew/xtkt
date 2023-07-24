```
 __  __  ______  __  __   ______  
/\_\_\_\/\__  _\/\ \/ /  /\__  _\ 
\/_/\_\/\/_/\ \/\ \  _"-.\/_/\ \/ 
  /\_\/\_\ \ \_\ \ \_\ \_\  \ \_\ 
  \/_/\/_/  \/_/  \/_/\/_/   \/_/ 
                                  
```
![Release on Homebrew](https://github.com/5amCurfew/xtkt/actions/workflows/release.yml/badge.svg)
![Commit activity (branch)](https://img.shields.io/github/commit-activity/m/5amCurfew/xtkt)
[![Go Report Card](https://goreportcard.com/badge/github.com/5amCurfew/xtkt)](https://goreportcard.com/report/github.com/5amCurfew/xtkt)

- [Installation](#Installation)
- [Using with Singer.io Targets](#using-with-singerio-targets)
- [Metadata](#metadata)
- [Config.json](#configjson)
- [Examples](#examples)
  * [Rick & Morty API](#rick-&-morty-api)
  * [Github API](#github-api)
  * [Strava API](#strava-api)
  * [Postgres](#postgres)
  * [SQLite](#sqlite)
  * [www.fifaindex.com/teams](#wwwfifaindexcomteams)
  * [File](#file)
  * [Listen](#listen)

`xtkt` ("extract") is a data extraction tool that follows the Singer.io specification. Supported sources include RESTful-APIs, databases and files (csv, jsonl). HTML scraping in beta.

New **and updated** records are sent to your target as new records (unique key `_sdc_surrogate_key`).

A **bookmark** can be used to define which records are processed by `xtkt` and subsequently sent to your target. A bookmark can be either a field within the records indicating the latest record processed (e.g. `updated_at`) or *new-record-detection* (`records.primary_bookmark: [*]`) to only process new/updated records (*new-record-detection* is not advised for large data sets).

In the absence of a bookmark, all records will be processed and sent to your target. This may be suitable if you want to detect hard-deletion in your data model (using `_sdc_time_extracted`).

`xtkt` can also listen for incoming messages (designed for webhooks) and continuously pipe them to your target.

Fields can be dropped from records prior to being sent to your target using the `records.drop_field_paths` field in your JSON configuration file (see examples below).

Sensitive data fields can be hashed within records prior to being sent to your target using the `records.sensitive_field_paths` field in your JSON configuration file (see examples below).

Intelligent data fields (REMOVED FOR NOW) can be added to your records using OpenAI LLM models using the `records.intelligent_fields` field in your JSON configuration file (see examples below, requires environment variable `OPENAI_API_KEY`).

`xtkt` can be pipe'd to any target that meets the Singer.io specification but has been designed and tested for databases such as SQLite, Postgres and BigQuery. Each stream is handled independently and deletion-at-source is not detected.

`xtkt` is still in development (currently v0.0.7)

### Installation

Locally: `git clone git@github.com:5amCurfew/xtkt.git`; `go build`

via Homebrew: `brew tap 5amCurfew/5amCurfew; brew install 5amCurfew/5amCurfew/xtkt`

```bash
$ xtkt --help
xtkt is a command line interface to extract data from a RESTful API or database to pipe to any target that meets the Singer.io specification

Usage:
  xtkt <PATH_TO_CONFIG_JSON> [flags]

Flags:
  -h, --help      help for xtkt
  -v, --version   version for xtkt
```

### Using with [Singer.io](https://www.singer.io/) Targets

Install targets (Python) in `_targets/` in virtual environments:

  1. `python3 -m venv ./_targets/target-name`
  2. `source ./_targets/target-name/bin/activate`
  3. `python3 -m pip install target-name`
  4. `deactivate`

```bash
xtkt config.json | ./_targets/target-name/bin/target-name`
```

For example:
```bash
xtkt config_github.json | ./_targets/pipelinewise-target-postgres/bin/target-postgres -c config_target_postgres.json 
```

I have been using [jq](https://github.com/stedolan/jq) to view `stdout` messages in development. For example:
```bash
$ xtkt config_github.json 2>&1 | jq .
```

### Metadata

`xtkt` adds the following metadata to records

* `_sdc_surrogate_key`: SHA256 of a record
* `_sdc_natural_key`: the unique key identifier of the record at source
* `_sdc_time_extracted`: a timestamp (R3339) at the time of the data extraction

### Config.json

#### xtkt
```javascript
{
    "stream_name": "<stream_name>", // required, <string>: the name of your stream
    "source_type": "<source_type>", // required, <string>: one of either db, file, html, rest or listen
    "url": "<url>", // required, <string>: address of the data source (e.g. REST-ful API address, database connection URL, relative file path etc)
    "records": { // required <object>: describes handling of records
        "unique_key_path": ["<key_path_1>", "<key_path_2>", ...], // required <array[string]>: path to unique key of records
        "primary_bookmark_path": ["<key_path_1>", "<key_path_1>", ...], // optional <array[string]>: path to bookmark within records
        "drop_field_paths": [ // optional <array[array]>: paths to remove within records
            ["<key_path_1>", "<key_path_1>", ...], // required <array[string]>
            ...
        ],
        "sensitive_field_paths": [ // optional <array[array]>: array of paths of fields to hash
            ["<sensitive_path_1_1>", "<sensitive_path_1_2>", ...], // required <array[string]>
            ...
        ],
    }
    ...
```

#### db, html and listen
```javascript
    ...
    "db": { // optional <object>: required when "source_type": "db"
        "table": "<table>" // required <string>: table name in database
    },
    "html": { // optional <object>: required when "source_type": "html"
        "elements_path": "<elements_path>", // required <string>: css identifier of elements parent
        "elements": [ // required <array[object]>
            {
            "name": "<element_name>", // required <string>: resulting field name in record
            "path": "<element_path>" // required <string>: css identifier of record
            },
            ...
        ]
    },
    "listen": { // optional <object>: required when "source_type": "listen"
        "collection_interval": "<collection_interval>", // required <int>: period of collection in seconds before emitting record messages
        "port": "<port>" // required <string>: port declaration of xtkt API
    },
    ...
```

#### rest
```javascript
    ...
    "rest": { // optional <object>: required when "source_type": "rest"
        "sleep": "<sleep>", // required <int>: number of seconds between pagination requests
        "auth": { // optional <object>: describe the authorisation strategy
            "required": "<required>", // required <boolean>: is authorisation required?
            "strategy": "<strategy>", // optional <string>: required if "required": true, one of either basic, token or oauth
            "basic": { // optional <object>: required if "strategy": "basic"
                "username": "<username>", // required <string>
                "password": "<password>" // required <string>
            },
            "token": { // optional <object>: required if "strategy": "token"
                "header": "<header>", // required <string>: authorisation header name
                "header_value": "<header_value>" // required <string> authorisation header value
            },
            "oauth": { // optional <object>: required if "strategy": "oauth"
                "client_id": "<client_id>", // required <string>
                "client_secret": "<client_secret>", // required <string>
                "refresh_token": "<refresh_token>", // required <string>
                "token_url": "<token_url>" // required <string>
            }
        },
        "response": { // required <object>: describes the REST-ful API response handling
            "records_path": ["<records_path_1>", "<records_path_2>", ...], // optional <string>: path to records in response (omit if immediately returned)
            "pagination": "<pagination>", // required <boolean>: is there pagination in the response?
            "pagination_strategy": "<pagination_strategy>", // optional <string>: required if "pagination": true, one of either "next" or "query"
            "pagination_next_path": ["<pagination_next_path_1>", "<pagination_next_path_2>", ...], // optional <array[string]>: required if "pagination_strategy": "next", path to "next" URL in response
            "pagination_query": { // optional <object>: required if "pagination_strategy": "query", describes pagination query strategy
                "query_parameter": "<query_parameter>", // required <string>: parameter name for URL pagination
                "query_value": "<query_value>", // required <int>: initial value after base URL is called
                "query_increment": "<query_increment>" // required <int>: query parameter increment
            }
        }
    }
    ...
```

### Examples

#### [Rick & Morty API](https://rickandmortyapi.com/)
No authentication required, records found in the response "results" array, paginated using "next", *new-record-detection* used for bookmark

`config.json`
```json
{
    "stream_name": "rick_and_morty_characters",
    "source_type": "rest",
    "url": "https://rickandmortyapi.com/api/character",
    "records": {
        "unique_key_path": ["id"],
        "primary_bookmark_path": ["*"],
        "drop_field_paths": [
            ["episode"],
            ["origin", "url"]
        ],
        "filter_field_paths": [
            {
                "field_path": ["gender"],
                "operation": "equal_to",
                "value": "Female"
            }
        ],
        "sensitive_field_paths": [
            ["name"],
            ["location", "name"]
        ]
    },
    "rest": {
        "sleep": 0,
        "auth": {
            "required": false
        },
        "response": {
            "records_path": ["results"],
            "pagination": true,
            "pagination_strategy": "next",
            "pagination_next_path": ["info", "next"]
        }
    }
}
```

#### [Github API](https://docs.github.com/en/rest?apiVersion=2022-11-28)
Token authentication required, records returned immediately as an array, pagination using query parameter, bookmark'd using "commit.author.date" in record

`config.json`
```json
{
    "stream_name": "xtkt_github_commits",
    "source_type": "rest",
    "url": "https://api.github.com/repos/5amCurfew/xtkt/commits",
    "records": {
        "unique_key_path": ["sha"],
        "primary_bookmark_path": ["commit", "author", "date"],
        "drop_field_paths": [
            ["author"],
            ["committer", "avatar_url"],
            ["committer", "events_url"]
        ],
        "sensitive_field_paths": [
            ["commit", "author", "email"],
            ["commit", "committer", "email"]
        ]
    },
    "rest": {
        "auth": {
            "required": true,
            "strategy": "token",
            "token": {
                "header": "Authorization",
                "header_value": "Bearer YOUR_GITHUB_API_TOKEN"
            }
        },
        "response": {
            "pagination": true,
            "pagination_strategy": "query",
            "pagination_query": {
                "query_parameter": "page",
                "query_value": 2,
                "query_increment": 1
            }
        }
    }
}
```

#### [Strava API](https://developers.strava.com/docs/reference/)
Oauth authentication required, records returned immediately in an array, paginated using query parameter, bookmark'd using "start_date" in record

`config.json`
```json
{
    "stream_name": "my_strava_activities",
    "source_type": "rest",
    "url": "https://www.strava.com/api/v3/athlete/activities",
    "records": {
        "unique_key_path": ["id"],
        "primary_bookmark_path": ["start_date"]
    },
    "rest": {
        "auth": {
            "required": true,
            "strategy": "oauth",
            "oauth": {
                "client_id": "YOUR_CLIENT_ID",
                "client_secret": "YOUR_CLIENT_SECRET",
                "refresh_token": "YOUR_REFRESH_TOKEN",
                "token_url": "https://www.strava.com/oauth/token"
            }
        },
        "sleep": 1,
        "response": {
            "pagination": true,
            "pagination_strategy": "query",
            "pagination_query": {
                "query_parameter": "page",
                "query_value": 2,
                "query_increment": 1
            }
        }
    }
}
```

#### Postgres
`config.json`
```json
{
    "stream_name": "rick_and_morty_characters_from_postgres",
    "source_type": "db",
    "url": "postgres://admin:admin@localhost:5432/postgres?sslmode=disable",
    "records": {
        "unique_key_path": ["id"]
    },
    "db": {
        "table": "rick_and_morty_characters"
    }
}
```

#### SQLite
`config.json`
```json
{
    "stream_name": "sqlite_customers",
    "source_type": "db",
    "url": "sqlite:///example.db",
    "records": {
        "unique_key_path": ["id"],
        "primary_bookmark_path": ["updated_at"]
    },
    "db": {
        "table": "customers"
    }
}
```

#### [www.fifaindex.com/teams](https://www.fifaindex.com/teams/)
Scrape team "overall" rating found within HTML table (beta)

`config.json`
```json
{
    "stream_name": "fifa_team_ratings",
    "source_type": "html",
    "url": "https://www.fifaindex.com/teams/",
    "records": {
        "unique_key_path": ["name"]
    },
    "html": {
        "elements_path": "table.table-teams > tbody > tr",
        "elements": [
            {"name": "name", "path": "td[data-title='Name'] > a.link-team"},
            {"name": "league", "path": "td[data-title='League'] > a.link-league"},
            {"name": "overall", "path": "td[data-title='OVR'] > span.rating:nth-child(1)"}
        ]
    }
}
```

#### File
`config.json`
```json
{
    "stream_name": "xtkt_jsonl",
    "source_type": "file",
    "url": "data.jsonl",
    "records": {
        "unique_key_path": ["name"],
        "sensitive_field_paths": [
            ["name"]
        ]
    }
}
```

#### Listen
`config.json` (e.g. `curl -X POST -H "Content-Type: application/json" -d '{"key1":"value1","key2":"value2"}' http://localhost:8080/messages`)
```json
{
    "stream_name": "listen_testing",
    "source_type": "listen",
    "url": "",
    "records": {
        "unique_key_path": ["key1"],
        "sensitive_field_paths": [
            ["key2"]
        ]
    },
    "listen":{
        "collection_interval": 10,
        "port": "8080"
    }
}
```