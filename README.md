```
 __  __  ______  __  __   ______  
/\_\_\_\/\__  _\/\ \/ /  /\__  _\ 
\/_/\_\/\/_/\ \/\ \  _"-.\/_/\ \/ 
  /\_\/\_\ \ \_\ \ \_\ \_\  \ \_\ 
  \/_/\/_/  \/_/  \/_/\/_/   \/_/ 
                                  
```
![Release on Homebrew](https://github.com/5amCurfew/xtkt/actions/workflows/release.yml/badge.svg)

- [:computer: Installation](#computer-installation)
- [:nut_and_bolt: Using with Singer.io Targets](#nut_and_bolt-using-with-singerio-targets)
- [:floppy_disk: Metadata](#floppy_disk-metadata)
- [:wrench: Config.json](#wrench-configjson)
- [:rocket: Examples](#rocket-examples)
  * [Rick & Morty API](#rick-&-morty-api)
  * [Github API](#github-api)
  * [Strava API](#strava-api)
  * [Postgres](#postgres)
  * [SQLite](#sqlite)
  * [File](#file)

**v0.1.7**

`xtkt` ("extract") is an opinionated data extraction tool that follows the Singer.io specification. Supported sources include RESTful-APIs, databases and files (csv, jsonl).

`xtkt` can be pipe'd to any target that meets the Singer.io specification but has been designed and tested for databases such as SQLite & Postgres. Each stream is handled independently and deletion-at-source is not detected.

Extracted records are versioned, with new and updated data being treated as distinct records (with resulting keys `_sdc_natural_key` (unique key) and `_sdc_surrogate_key` (version key)). Only new and updated records are sent to be processed by your target.

Fields can be dropped from records prior to being sent to your target using the `records.drop_field_paths` field in your JSON configuration file (see examples below). This may be suitable for dropping redundant, large objects within a record.

Fields can be hashed within records prior to being sent to your target using the `records.sensitive_field_paths` field in your JSON configuration file (see examples below). This may be suitable for handling sensitive data.

Records are processed concurrently (once all records are gathered). Default ocncurrency is set to 1000 goroutines but can be set using the `max_concurrency` field in your JSON configuration file (see examples below).

Both integers and floats are sent as floats. All fields are considered `NULLABLE`. All fields when extracting from CSV are considered strings for now.

Schema detection is naive using the first data type detected per field used.

### :computer: Installation

Locally: `git clone git@github.com:5amCurfew/xtkt.git`; `make build`

via Homebrew : `brew tap 5amCurfew/5amCurfew; brew install 5amCurfew/5amCurfew/xtkt`

```bash
$ xtkt --help
xtkt is a command line interface to extract data from a RESTful API, database or files to pipe to any target that meets the Singer.io specification

Usage:
  xtkt [PATH_TO_CONFIG_JSON] [flags]

Flags:
  -h, --help          help for xtkt
      --save-schema   save the schema to a file after extraction
  -v, --version       version for xtkt
```

### :nut_and_bolt: Using with [Singer.io](https://www.singer.io/) Targets

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
xtkt config.json | ./_targets/pipelinewise-target-postgres/bin/target-postgres -c config_target_postgres.json 
```

I have been using [jq](https://github.com/stedolan/jq) to view `stdout` messages in development. For example:
```bash
$ xtkt config.json 2>&1 | jq .
```

`xtkt` can be used in a bash script to iterate over a template `config.json` file to create many data extractions. For example
```bash
#!/bin/bash

# Loop from 2009 to 2019
for year in {2009..2019}
do
    new_config="config_${year}.json"
    sed "s/YYYY/${year}/g" config.json.template > $new_config
    echo "Generated ${new_config}"
    echo "Running xtkt on ${new_config}"
    xtkt $new_config | ./_targets/target-name/bin/target-name -c config_target_name.json
done

rm -f state_* config_*
```

### :floppy_disk: Metadata

`xtkt` adds the following metadata to records

* `_sdc_surrogate_key`: SHA256 of a record
* `_sdc_natural_key`: the unique key identifier of the record at source
* `_sdc_time_extracted`: a timestamp (R3339) at the time of the data extraction

### :wrench: Config.json

#### xtkt
```javascript
{
    "stream_name": "<stream_name>", // required, <string>: the name of your stream
    "source_type": "<source_type>", // required, <string>: one of either csv, db, jsonl, html, rest
    "url": "<url>", // required, <string>: address of the data source (e.g. REST-ful API address, database connection URL or relative file path)
    "max_concurrency": "<max_thread_count>", // optional <int>: maximum number of records processed concurrently (default: 1000)
    "records": { // required <object>: describes handling of records
        "unique_key_path": ["<key_path_1>", "<key_path_2>", ...], // required <array[string]>: path to unique key of records
        "drop_field_paths": [ // optional <array[array]>: paths to remove within records
            ["<key_path_1>", "<key_path_2>", ...], // required <array[string]>
            ...
        ],
        "sensitive_field_paths": [ // optional <array[array]>: array of paths of fields to hash
            ["<sensitive_path_1_1>", "<sensitive_path_1_2>", ...], // required <array[string]>
            ["<sensitive_path_2_1>", "<sensitive_path_2_2>", ...],
            ...
        ],
    }
    ...
```

#### database
```javascript
    ...
    "db": { // optional <object>: required when "source_type": "db"
        "table": "<table>" // required <string>: table name in database
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

### :rocket: Examples

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
        "drop_field_paths": [
            ["episode"],
            ["origin", "url"]
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
        "unique_key_path": ["id"]
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
    "stream_name": "xtkt_github_commits_from_postgres",
    "source_type": "db",
    "url": "postgres://admin:admin@localhost:5432/postgres?sslmode=disable",
    "records": {
        "unique_key_path": ["_sdc_natural_key"]
    },
    "db": {
        "table": "xtkt_github_commits"
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
    },
    "db": {
        "table": "customers"
    }
}
```

#### File
`config.json`
```json
{
    "stream_name": "xtkt_jsonl",
    "source_type": "jsonl",
    "url": "_config_json/data.jsonl/csv",
    "max_concurrency": 50,
    "records": {
        "unique_key_path": ["id"],
        "sensitive_field_paths": [
            ["location", "address"],
            ["age"]
        ]
    }
}
```