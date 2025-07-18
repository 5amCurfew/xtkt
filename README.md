```
 __  __  ______  __  __   ______  
/\_\_\_\/\__  _\/\ \/ /  /\__  _\ 
\/_/\_\/\/_/\ \/\ \  _"-.\/_/\ \/ 
  /\_\/\_\ \ \_\ \ \_\ \_\  \ \_\ 
  \/_/\/_/  \/_/  \/_/\/_/   \/_/ 
                                  
```
![Release on Homebrew](https://github.com/5amCurfew/xtkt/actions/workflows/release.yml/badge.svg)

- [:computer: Installation](#computer-installation)
- [:floppy_disk: Metadata](#floppy_disk-metadata)
- [:pencil: Catalog](#pencil-catalog)
- [:clipboard: State](#clipboard-state)
- [:nut_and_bolt: Using with Singer.io Targets](#nut_and_bolt-using-with-singerio-targets)
- [:wrench: Config.json](#wrench-configjson)
- [:rocket: Examples](#rocket-examples)
  * [Rick & Morty API](#rick-&-morty-api)
  * [Github API](#github-api)
  * [Strava API](#strava-api)
  * [Salesforce API](#salesforce-api)
  * [File csv](#file-csv)
  * [File jsonl](#file-jsonl)

**v0.6.0**

`xtkt` ("extract") is a data extraction tool that follows the Singer.io specification. Supported sources include RESTful APIs, csv and jsonl.

`xtkt` can be pipe'd to any target that meets the Singer.io specification but has been designed and tested for databases such as SQLite & Postgres. Each stream is handled independently and deletion-at-source is not detected.

Extracted records are versioned, with new and updated data being treated as distinct records (with resulting keys `_sdc_surrogate_key` (SHA256 hash of the record), `_sdc_unique_key` (unique identifier for the extraction, combining `_sdc_surrogate_key` and `_sdc_timestamp`), and `_sdc_natural_key` (unique identifier in the source system)). By default (incremental) only new and updated records are sent to be processed by your target. For a full refresh of the stream, use the `--refresh` flag.

Fields can be dropped from records prior to being sent to your target using the `records.drop_field_paths` field in your JSON configuration file (see examples below). This may be suitable for dropping redundant, large objects within a record.

Fields can be hashed within records prior to being sent to your target using the `records.sensitive_field_paths` field in your JSON configuration file (see examples below). This may be suitable for handling sensitive data.

Both integers and floats are sent as floats. All fields except `records.unique_key_path` field are considered `NULLABLE`.

### :computer: Installation

Locally: `git clone git@github.com:5amCurfew/xtkt.git`; `make build`

via Homebrew : `brew tap 5amCurfew/5amCurfew; brew install 5amCurfew/5amCurfew/xtkt`

```bash
$ xtkt --help
xtkt is a command line interface to extract data from RESTful APIs, CSVs, and JSONL files to pipe to any target that meets the Singer.io specification.

Usage:
  xtkt [PATH_TO_CONFIG_JSON] [flags]

Flags:
  -d, --discover   run the tap in discovery mode, creating the catalog
  -h, --help       help for xtkt
  -r, --refresh    extract all records (full refresh) rather than only new or modified records (incremental, default)
  -v, --version    version for xtkt
```

### :floppy_disk: Metadata

`xtkt` adds the following metadata to records

* `_sdc_natural_key`: Unique identifier of the record in the source system.
* `_sdc_surrogate_key`: SHA256 hash of the record for secure identification.
* `_sdc_timestamp`: Timestamp (RFC 3339) of when the data was extracted.
* `_sdc_unique_key`: Unique identifier for the specific extraction of the record.

### :pencil: Catalog

A [catalog](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md#catalog) is required for the extraction for schema validation. Discovery of the catalog can be run using the `--discover` flag which infers and creates the `<stream_name>_catalog.json` file. This can then be altered for required specification. This schema is read and sent as the [*schema message*](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md#schema-message) to your target. Running `xtkt` in `--discovery` will update an existing catalog if new properties are detected in records extracted.

Schema detection is naive using the data type of the first non-null value detected per property when generating the catalog.

```bash
$ xtkt config.json --discover
```

### :clipboard: State

`xtkt` uses a state file to track the last detected `_sdc_surrogate_key` per `_sdc_natural_key`. The state file is written to the current working directory and is named `<stream_name>_state.json` where the `bookmark` holds the latest `_sdc_surrogate_key` per `_sdc_natural_key`. Records that fail schema validation are skipped.

### :nut_and_bolt: Using with [Singer.io](https://www.singer.io/) Targets

Install targets (Python) in `_targets/` in virtual environments:

  1. `python3 -m venv ./_targets/target-name`
  2. `source ./_targets/target-name/bin/activate`
  3. `python3 -m pip install target-name`
  4. `deactivate`

```bash
xtkt config.json | ./_targets/target-name/bin/target-name` --config config_target.json
```

For example:
```bash
xtkt config.json | ./_targets/pipelinewise-target-postgres/bin/target-postgres -c config_target_postgres.json 
```

For debugging I suggest pipe'ing to [jq](https://github.com/stedolan/jq) to view `stdout` messages in development. For example:
```bash
$ xtkt config.json 2>&1 | jq .
```

### :wrench: Config.json

#### xtkt
```javascript
{
    "stream_name": "<stream_name>", // required, <string>: the name of your stream
    "source_type": "<source_type>", // required, <string>: one of either csv, db, jsonl, html, rest
    "url": "<url>", // required, <string>: address of the data source (e.g. REST-ful API address or relative file path)
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

#### rest
```javascript
    ...
    "rest": { // optional <object>: required when "source_type": "rest"
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
No authentication required, records found in the response "results" array, paginated using "next"

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
Token authentication required, records returned immediately as an array, pagination using query parameter

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
Oauth authentication required, records returned immediately in an array, paginated using query parameter

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

#### Salesforce API
Oauth authentication required, records found in the response "records" array, no pagination

```json
{
    "stream_name": "salesforce_accounts",
    "source_type": "rest",
    "url": "https://livescore.my.salesforce.com/services/data/v62.0/query/?q=SELECT+name,id+from+Account",
    "records": {
        "unique_key_path": ["Id"]
    },
    "rest": {
        "auth": {
            "required": true,
            "strategy": "oauth",
            "oauth": {
                "client_id": "<YOUR_CONSUMER_KEY>",
                "client_secret": "<YOUR_CONSUMER_SECRET>",
                "refresh_token": "<YOUR_REFRESH_TOKEN>",
                "token_url": "https://login.salesforce.com/services/oauth2/token"
            }
        },
        "response": {
            "records_path": ["records"],
            "pagination": false
        }
    }
}
```

#### File csv
```json
{
    "stream_name": "xtkt_csv",
    "source_type": "csv",
    "url": "_config/data.csv",
    "records": {
        "unique_key_path": ["name"],
        "sensitive_field_paths": [
            ["age"],
            ["city"]
        ]
    }
}
```

#### File jsonl
`config.json`
```json
{
    "stream_name": "xtkt_jsonl",
    "source_type": "jsonl",
    "url": "_config_json/data.jsonl",
    "records": {
        "unique_key_path": ["id"],
        "drop_field_paths": [
            ["salary"]
        ],
        "sensitive_field_paths": [
            ["location", "address"],
            ["age"]
        ]
    }
}
```