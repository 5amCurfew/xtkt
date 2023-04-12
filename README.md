```
 __  __  ______  __  __   ______  
/\_\_\_\/\__  _\/\ \/ /  /\__  _\ 
\/_/\_\/\/_/\ \/\ \  _"-.\/_/\ \/ 
  /\_\/\_\ \ \_\ \ \_\ \_\  \ \_\ 
  \/_/\/_/  \/_/  \/_/\/_/   \/_/ 
                                  
```

`xtkt` ("extract") is a data extraction tool that adheres to the Singer.io specification. At its core, `xtkt` takes an opinionated approach to ELT for OLAP importing updated data as a new record when a bookmark is provided (using either the bookmark or new-record-detection) for any RESTful API, database or web page. Streams are always handled independently and deletion at source is not detected. `xtkt` can be pipe'd to any target that meets the Singer.io specification but has been designed and tested for databases such as Postgres and BigQuery.

### Using with Singer.io Targets

Install targets (python) in `_targets/` in virtual environments:

  1. python3 -m venv ./_targets/target-name
  2. source ./_targets/target-name/bin/activate
  3. python3 -m pip install target-name
  4. deactivate

Usage: `xtkt config.json | ./_targets/target-name/bin/target-name`

  * Postgres: 
    * `docker pull postgres`
    * `docker run --name pg_dev -e POSTGRES_USER=admin -e POSTGRES_PASSWORD=admin -p 5432:5432 -d postgres`
    * `docker start pg_dev`

* `xtkt config_full.json | ./_targets/pipelinewise-target-postgres/bin/target-postgres -c config_target_postgres.json`
* `xtkt config_full.json | ./_targets/target-sqlite/bin/target-sqlite -c config_target_sqlite.json`


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
            }
        ]
    }
}
```