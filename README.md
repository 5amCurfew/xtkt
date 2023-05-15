```
 __  __  ______  __  __   ______  
/\_\_\_\/\__  _\/\ \/ /  /\__  _\ 
\/_/\_\/\/_/\ \/\ \  _"-.\/_/\ \/ 
  /\_\/\_\ \ \_\ \ \_\ \_\  \ \_\ 
  \/_/\/_/  \/_/  \/_/\/_/   \/_/ 
                                  
```

- [Installation](#Installation)
- [Using with Singer.io Targets](#using-with-singerio-targets)
- [Metadata](#metadata)
- [Examples](#examples)
  * [Rick & Morty API](#rick-&-morty-api)
  * [Github API](#github-api)
  * [Strava API](#strava-api)
  * [Postgres](#postgres)
  * [SQLite](#sqlite)
  * [www.fifaindex.com/teams](#wwwfifaindexcomteams)

`xtkt` ("extract") is a data extraction tool that adheres to the Singer.io specification. At its core, `xtkt` takes an opinionated approach to ELT for OLAP importing updated data as a new record when a bookmark is provided (using either the bookmark or new-record-detection) for RESTful APIs, databases or web-pages. Sensitive data fields can be hashed before ingestion using the `records.sensitive_fields` field in your config json file. Streams are always handled independently and deletion at source is not detected. `xtkt` can be pipe'd to any target that meets the Singer.io specification but has been designed and tested for databases such as SQLite, Postgres and BigQuery.

`xtkt` is still in development (currently v0.0.7) and isn't advised for production at this time

Databases currently focussed on during development are Postgres, SQLite, MySQL & Microsoft-SQL-Server.

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

### Using with Singer.io Targets

Install targets (Python) in `_targets/` in virtual environments:

  1. `python3 -m venv ./_targets/target-name`
  2. `source ./_targets/target-name/bin/activate`
  3. `python3 -m pip install target-name`
  4. `deactivate`

`xtkt config.json | ./_targets/target-name/bin/target-name`

I have been using [jq](https://github.com/stedolan/jq) to view `stdout` messages in development. For example:
```bash
$ xtkt config_github.json 2>&1 | jq .
```

When there is not an appropriate bookmark but you want to only write updates to your target you can use new-record-detection (not advisable for large data sets) by setting the `records.primary_bookmark_path: ["*"]` in your `config.json`. See examples below

You can also hash fields (e.g. sensitive data) by setting the `records.sensitive_paths` field in your `config.json`. See examples below

When a bookmark is not provided (i.e. `records.bookmark: false`) full-table replication is invoked. For more information on replication methods see @transferwise's [Pipelinewise](https://github.com/transferwise/pipelinewise) documentation [here](https://transferwise.github.io/pipelinewise/concept/replication_methods.html)

### Metadata

`xtkt` adds the following metadata to records

* `_sdc_surrogate_key`: an identifier of a record (SHA256) generated using the unique_key and, if provided, bookmark. If record detection is used, this is generated using the entire record object
* `_sdc_natural_key`: the unique key identifier of the source data (set in the `records.unique_key_path` in `config.json`)
* `_sdc_time_extracted`: a timestamp (R3339) at the time of the data extraction

### Examples

#### [Rick & Morty API](https://rickandmortyapi.com/)
No authentication required, records found in the response "results" array, paginated using "next", new-record-detection used

```json
{
    "stream_name": "rick_and_morty_characters",
    "source_type": "rest",
    "url": "https://rickandmortyapi.com/api/character",
    "records": {
        "unique_key_path": [
            "id"
        ],
        "bookmark": true,
        "primary_bookmark_path": [
            "*"
        ]
    },
    "rest": {
        "auth": {
            "required": false
        },
        "response": {
            "records_path": [
                "results"
            ],
            "pagination": true,
            "pagination_strategy": "next",
            "pagination_next_path": [
                "info",
                "next"
            ]
        }
    }
}
```

#### [Github API](https://docs.github.com/en/rest?apiVersion=2022-11-28)
Token authentication required, records returned immediately as an array, pagination using query parameter, bookmark'd using "commit.author.date"

```json
{
    "stream_name": "xtkt_github_commits",
    "source_type": "rest",
    "url": "https://api.github.com/repos/5amCurfew/xtkt/commits",
    "records": {
        "unique_key_path": [
            "sha"
        ],
        "bookmark": true,
        "primary_bookmark_path": [
            "commit",
            "author",
            "date"
        ],
        "sensitive_paths": [
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
                "query_value": 1,
                "query_increment": 1
            }
        }
    }
}
```

#### [Strava API](https://developers.strava.com/docs/reference/)
Oauth authentication required, records returned immediately in an array, paginated using query parameter, bookmark'd using "start_date"

```json
{
    "stream_name": "my_strava_activities",
    "source_type": "rest",
    "url": "https://www.strava.com/api/v3/athlete/activities",
    "records": {
        "unique_key_path": [
            "id"
        ],
        "bookmark": true,
        "primary_bookmark_path": [
            "start_date"
        ]
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
                "query_value": 1,
                "query_increment": 1
            }
        }
    }
}
```

#### Postgres
```json
{
    "stream_name": "rick_and_morty_characters_from_postgres",
    "source_type": "db",
    "url": "postgres://admin:admin@localhost:5432/postgres?sslmode=disable",
    "records": {
        "unique_key_path": [
            "id"
        ],
        "bookmark": true,
        "primary_bookmark_path": ["created"],
        "sensitive_paths": [
            ["image"]
        ]
    },
    "db": {
        "table": "rick_and_morty_characters"
    }
}
```

#### SQLite
```json
{
    "stream_name": "sqlite_customers",
    "source_type": "db",
    "url": "sqlite:///example.db",
    "records": {
        "unique_key_path": [
            "id"
        ],
        "bookmark": true,
        "primary_bookmark_path": [
            "updated_at"
        ]
    },
    "db": {
        "table": "customers"
    }
}
```

#### [www.fifaindex.com/teams](https://www.fifaindex.com/teams/)
Scrape team "overall" rating found within HTML table (beta)

```json
{
    "stream_name": "fifa_team_ratings",
    "source_type": "html",
    "url": "https://www.fifaindex.com/teams/",
    "records": {
        "unique_key_path": [
            "name"
        ],
        "bookmark": false
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