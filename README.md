```
 __  __  ______  __  __   ______  
/\_\_\_\/\__  _\/\ \/ /  /\__  _\ 
\/_/\_\/\/_/\ \/\ \  _"-.\/_/\ \/ 
  /\_\/\_\ \ \_\ \ \_\ \_\  \ \_\ 
  \/_/\/_/  \/_/  \/_/\/_/   \/_/ 
                                  
```

`xtkt` is a command line interface to extract data from a REST API using the Singer.io Specification

TODO:

1. Handle Pagination
    * `limit` - parameter specifying the number of items to return per page
    * :white_check_mark: `next` - APIs may use different field names, such as next, nextLink, nextPage, or others, to indicate the URL for the next page of results
    * `offset`- parameter specifying the starting position of the data to return. For example, if offset=10, the API will skip the first 10 items and return the next set of items

2. Handle authorisation

    * :white_check_mark: API Key: This involves providing a unique key to the user or application, which is used to authenticate API requests. This key is usually included in the header or query parameters of the request.
    * OAuth 2.0: This is a widely used authorization framework that allows users or applications to access protected resources on behalf of a user. OAuth 2.0 works by providing an access token that is used to authenticate API requests.
    * JSON Web Tokens (JWT): JWT is a self-contained token that contains user or application information, which can be used to authenticate API requests. JWTs are signed and encrypted, providing a secure method of authentication.
    * :white_check_mark: Basic Authentication: This involves using a username and password to authenticate API requests. The credentials are usually passed in the header of the request, encoded in Base64.

3. Test with targets

Install targets in `_targets/` in virtual environments:

  1. python3 -m venv ./_targets/target-name
  2. source ./_targets/target-name/bin/activate
  3. python3 -m pip install target-name
  4. deactivate

Usage: `xtkt config.json | ./_targets/target-name/bin/target-name`

  * Postgres: `docker pull postgres`, `docker run --name pg_dev -e POSTGRES_USER=admin -e POSTGRES_PASSWORD=admin -p 5432:5432 -d postgres`
    * `xtkt config_token.json | ./_targets/pipelinewise-target-postgres/bin/target-postgres -c pg_dev.json`