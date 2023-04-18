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
- [config.json template](#configjson-template)

`xtkt` ("extract") is a data extraction tool that adheres to the Singer.io specification. At its core, `xtkt` takes an opinionated approach to ELT for OLAP importing updated data as a new record when a bookmark is provided (using either the bookmark or new-record-detection) for any RESTful API, database or web page. Sensitive data fields can be hashed before ingestion using the `records.sensitive_fields` field in your config file. Streams are always handled independently and deletion at source is not detected. `xtkt` can be pipe'd to any target that meets the Singer.io specification but has been designed and tested for databases such as SQLite, Postgres and BigQuery.

`xtkt` is still in development (currently v0.0.5) and isn't advised for production at this time

### Installation

Locally: `git clone git@github.com:5amCurfew/xtkt.git`; `go build`

via Homebrew: `brew tap 5amCurfew/5amCurfew; brew install 5amCurfew/5amCurfew/xtkt`

### Using with Singer.io Targets

Install targets (Python) in `_targets/` in virtual environments:

  1. `python3 -m venv ./_targets/target-name`
  2. `source ./_targets/target-name/bin/activate`
  3. `python3 -m pip install target-name`
  4. `deactivate`

`xtkt config.json | ./_targets/target-name/bin/target-name`

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
    "source_type": "database",
    "url": "postgres://admin:admin@localhost:5432/postgres?sslmode=disable",
    "records": {
        "unique_key_path": [
            "id"
        ],
        "bookmark": false
    },
    "database": {
        "table": "rick_and_morty_characters"
    }
}
```

#### SQLite
```json
{
    "stream_name": "sqlite_customers",
    "source_type": "database",
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
    "database": {
        "table": "customers"
    }
}
```

#### [www.fifaindex.com/teams](https://www.fifaindex.com/teams/)
Scrape team "overall" rating found within HTML table

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


### config.json template

```json
{
    "stream_name": "name_of_this_datastream",
    "source_type": "rest",
    "url": "https://www.helloworld.com/route",
    "records": {
        "unique_key_path": [
            "id"
        ],
        "bookmark": true,
        "primary_bookmark_path": [
            "updated_at"
        ],
        "sensitive_paths": [
            ["commit", "author", "email"]
        ]
    },
    "database": {
        "table": "my_table"
    },
    "rest": {
        "auth": {
            "required": true,
            "strategy": "token",
            "basic": {
                "username": "u",
                "password": "p"
            },
            "token": {
                "header": "Authorization",
                "header_value": "Bearer YOUR_API_TOKEN"
            },
            "oauth": {
                "client_id": "YOUR_OAUTH_CLIENT_ID",
                "client_secret": "YOUR_OAUTH_CLIENT_SECRET",
                "refresh_token": "YOUR_OAUTH_REFRESH_TOKEN",
                "token_url": "OAUTH_TOKEN_URL"
            }
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
            ],
            "pagination_query": {
                "query_parameter": "page",
                "query_value": 1,
                "query_increment": 1
            }
        }
    },
    "html": {
        "elements_path": "table.table-teams > tbody > tr",
        "elements": [
            {
                "name": "name",
                "path": "td[data-title='Name'] > a.link-team"
            },
            {
                "name": "league",
                "path": "td[data-title='League'] > a.link-league"
            },
            {
                "name": "overall",
                "path": "td[data-title='OVR'] > span.rating:nth-child(1)"
            }
        ]
    }
}
```