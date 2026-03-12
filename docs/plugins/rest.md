# REST API

The `rivetsql-rest` plugin treats REST API endpoints as tables. Handles authentication, pagination, response parsing, schema inference, and Arrow conversion automatically. Works with any Arrow-compatible engine (DuckDB, Polars, PySpark) through wildcard adapter architecture.

```bash
pip install 'rivetsql[rest]'
```

---

## Catalog Configuration

```yaml
default:
  engines:
    - name: local
      type: duckdb
      catalogs: [my_api]
  catalogs:
    - name: my_api
      type: rest_api
      options:
        base_url: https://api.example.com/v1
        auth: bearer
        token: ${REST_API_TOKEN}
        default_headers:
          Accept: application/json
        timeout: 30
        response_format: json
        max_flatten_depth: 3
        rate_limit:
          requests_per_second: 10
          burst: 5
        max_retries: 3
        endpoints:
          users:
            path: /users
            method: GET
            response_path: data.users
            pagination:
              strategy: offset
              limit: 100
              offset_param: offset
              limit_param: limit
            filter_params:
              status: status
              created_after: since
          orders:
            path: /orders
            method: GET
            response_path: data
            pagination:
              strategy: cursor
              cursor_field: next_cursor
              cursor_param: cursor
```

### Catalog Options

| Option | Required | Type | Default | Description |
|--------|----------|------|---------|-------------|
| `base_url` | Yes | `str` | - | Root URL of the API (e.g. `https://api.example.com/v1`) |
| `auth` | No | `str` | `"none"` | Authentication strategy: `none`, `bearer`, `basic`, `api_key`, `oauth2` |
| `default_headers` | No | `dict` | `{}` | Headers applied to every request |
| `timeout` | No | `int` | `30` | Request timeout in seconds |
| `response_format` | No | `str` | `"json"` | Response format: `json` or `csv` |
| `max_flatten_depth` | No | `int` | `3` | Maximum nesting depth for JSON flattening |
| `rate_limit` | No | `dict` | `None` | Rate limiting configuration |
| `max_retries` | No | `int` | `3` | Maximum retry attempts for transient errors |
| `endpoints` | No | `dict` | `{}` | Endpoint table configurations |

### Credential Options

Credentials vary by authentication strategy:

| Auth Strategy | Required Credentials | Optional |
|---------------|---------------------|----------|
| `none` | - | - |
| `bearer` | `token` | - |
| `basic` | `username`, `password` | - |
| `api_key` | `api_key_value` | `api_key_name`, `api_key_location` |
| `oauth2` | `client_id`, `client_secret`, `token_url` | - |

### Environment Variable Hints

| Credential | Suggested Env Var |
|------------|-------------------|
| `token` | `REST_API_TOKEN` |
| `username` | `REST_API_USERNAME` |
| `password` | `REST_API_PASSWORD` |
| `api_key_value` | `REST_API_KEY` |
| `client_id` | `REST_API_CLIENT_ID` |
| `client_secret` | `REST_API_CLIENT_SECRET` |

---

## Endpoint Configuration

Each endpoint represents a logical table. Configure endpoints in the catalog's `endpoints` option:

```yaml
endpoints:
  users:
    path: /users
    method: GET
    response_path: data.users
    pagination:
      strategy: offset
      limit: 100
    filter_params:
      status: status
      created_after: since
```

### Endpoint Options

| Option | Required | Type | Default | Description |
|--------|----------|------|---------|-------------|
| `path` | Yes | `str` | - | URL path relative to `base_url` (e.g. `/users`) |
| `method` | No | `str` | `"GET"` | HTTP method: `GET`, `POST`, `PUT`, `PATCH`, `DELETE` |
| `params` | No | `dict` | `{}` | Default query parameters for this endpoint |
| `headers` | No | `dict` | `{}` | Endpoint-specific headers (merged with `default_headers`) |
| `body` | No | `any` | `None` | Request body template for POST/PUT/PATCH |
| `response_path` | No | `str` | `None` | Dot-separated JSON path to record array (e.g. `data.users`) |
| `pagination` | No | `dict` | `None` | Pagination configuration |
| `filter_params` | No | `dict` | `None` | Column-to-query-param mapping for predicate pushdown |
| `schema` | No | `dict` | `None` | Explicit schema (column → Arrow type) to skip inference |
| `write_method` | No | `str` | `None` | Override HTTP method for writes (e.g. `PATCH`) |
| `batch_size` | No | `int` | `1` | Rows per write request |

---

## Authentication

The REST API plugin supports five authentication strategies configured via the `auth` catalog option.

### No Authentication

```yaml
catalogs:
  - name: public_api
    type: rest_api
    options:
      base_url: https://api.example.com
      auth: none
```

### Bearer Token

```yaml
catalogs:
  - name: my_api
    type: rest_api
    options:
      base_url: https://api.example.com
      auth: bearer
      token: ${REST_API_TOKEN}
```

Attaches `Authorization: Bearer {token}` header to every request.

### Basic Authentication

```yaml
catalogs:
  - name: my_api
    type: rest_api
    options:
      base_url: https://api.example.com
      auth: basic
      username: ${REST_API_USERNAME}
      password: ${REST_API_PASSWORD}
```

Attaches `Authorization: Basic {base64(username:password)}` header.

### API Key

API keys can be sent as a header or query parameter:

=== "Header Mode"

    ```yaml
    catalogs:
      - name: my_api
        type: rest_api
        options:
          base_url: https://api.example.com
          auth: api_key
          api_key_name: X-API-Key
          api_key_value: ${REST_API_KEY}
          api_key_location: header
    ```

=== "Query Parameter Mode"

    ```yaml
    catalogs:
      - name: my_api
        type: rest_api
        options:
          base_url: https://api.example.com
          auth: api_key
          api_key_name: api_key
          api_key_value: ${REST_API_KEY}
          api_key_location: query
    ```

Default location is `header` if not specified.

### OAuth2 Client Credentials

```yaml
catalogs:
  - name: my_api
    type: rest_api
    options:
      base_url: https://api.example.com
      auth: oauth2
      client_id: ${REST_API_CLIENT_ID}
      client_secret: ${REST_API_CLIENT_SECRET}
      token_url: https://auth.example.com/oauth/token
```

The plugin automatically:
- Exchanges credentials for an access token on first request
- Caches the token for subsequent requests
- Refreshes the token when it expires (within 30 seconds of expiry)

---

## Pagination

The REST API plugin supports five pagination strategies. Configure per endpoint:

### No Pagination

Single request, no page iteration:

```yaml
endpoints:
  status:
    path: /status
    # No pagination config = single request
```

### Offset/Limit


Increments `offset` by `limit` after each page. Stops when a page returns fewer records than `limit`:

```yaml
endpoints:
  users:
    path: /users
    pagination:
      strategy: offset
      limit: 100
      offset_param: offset  # Query param name for offset
      limit_param: limit    # Query param name for limit
```

Sends requests like:
- `GET /users?offset=0&limit=100`
- `GET /users?offset=100&limit=100`
- `GET /users?offset=200&limit=100`

Stops when a page returns < 100 records.

### Cursor-Based

Extracts next cursor from response field, passes as query parameter:

```yaml
endpoints:
  orders:
    path: /orders
    pagination:
      strategy: cursor
      cursor_field: next_cursor  # Response field containing next cursor
      cursor_param: cursor       # Query param name for cursor
```

Example response:
```json
{
  "data": [...],
  "next_cursor": "eyJpZCI6MTIzfQ=="
}
```

Sends:
- `GET /orders` (first page)
- `GET /orders?cursor=eyJpZCI6MTIzfQ==` (subsequent pages)

Stops when `next_cursor` is null or absent.

### Page Number

Increments page number starting at 1 (or configured start):

```yaml
endpoints:
  products:
    path: /products
    pagination:
      strategy: page_number
      page_size: 50
      page_param: page
      start_page: 1
      limit_param: limit
```

Sends:
- `GET /products?page=1&limit=50`
- `GET /products?page=2&limit=50`

Stops when a page returns < 50 records.

### Link Header (RFC 8288)

Follows `next` relation in HTTP `Link` header:

```yaml
endpoints:
  repos:
    path: /repos
    pagination:
      strategy: link_header
```

Example response header:
```
Link: <https://api.example.com/repos?page=2>; rel="next"
```

Stops when no `next` link is present.

---

## Response Parsing


### JSON Responses

The plugin automatically flattens nested JSON into Arrow-compatible columns.

#### Response Path Extraction

Use `response_path` to specify where records are located in the response:

```yaml
endpoints:
  users:
    path: /users
    response_path: data.users  # Dot-separated path
```

Example response:
```json
{
  "status": "success",
  "data": {
    "users": [
      {"id": 1, "name": "Alice"},
      {"id": 2, "name": "Bob"}
    ]
  }
}
```

The plugin extracts the array at `data.users`.

If `response_path` is not specified:
- JSON array → treated as record array
- JSON object → wrapped as single-row array

#### JSON Flattening

Nested objects are flattened using dot-separated column names:

```json
{
  "id": 1,
  "name": "Alice",
  "address": {
    "city": "NYC",
    "zip": "10001"
  }
}
```

Becomes columns:
- `id` (int64)
- `name` (utf8)
- `address.city` (utf8)
- `address.zip` (utf8)

Objects beyond `max_flatten_depth` (default 3) are JSON-serialized as strings.

#### Type Mapping

| JSON Type | Arrow Type | Notes |
|-----------|-----------|-------|
| string | utf8 | - |
| integer | int64 | - |
| float | float64 | - |
| boolean | bool | - |
| null | utf8 | Preserved as Arrow null |
| array | large_utf8 | JSON-serialized |
| nested object (beyond max depth) | large_utf8 | JSON-serialized |

#### Schema Evolution

When paginating across multiple pages:
- New columns in later pages → added with null backfill for earlier rows
- Type mismatches → coerced to inferred type, fallback to utf8 if coercion fails

### CSV Responses

```yaml
catalogs:
  - name: csv_api
    type: rest_api
    options:
      base_url: https://data.example.com
      response_format: csv
```

Uses PyArrow's CSV reader (`pyarrow.csv.read_csv`) for parsing.

---

## Predicate Pushdown


The plugin translates SQL filter conditions into API query parameters where supported.

### Configuration

Declare which columns can be pushed down in `filter_params`:

```yaml
endpoints:
  users:
    path: /users
    filter_params:
      status: status           # Column 'status' → query param 'status'
      created_after: since     # Column 'created_after' → query param 'since'
      email: email
```

### Supported Operators

| SQL Operator | Query Parameter |
|--------------|-----------------|
| `=` | `?column=value` |
| `<` | `?column_lt=value` |
| `>` | `?column_gt=value` |
| `<=` | `?column_lte=value` |
| `>=` | `?column_gte=value` |

### Example

SQL query:
```sql
SELECT * FROM users
WHERE status = 'active' AND created_after > '2024-01-01'
```

Becomes:
```
GET /users?status=active&since=2024-01-01
```

Unsupported predicates (e.g. `LIKE`, `IN`, columns not in `filter_params`) are applied as residual filters after fetching.

---

## Rate Limiting

Prevent overwhelming APIs with configurable rate limits and automatic retry:

```yaml
catalogs:
  - name: my_api
    type: rest_api
    options:
      base_url: https://api.example.com
      rate_limit:
        requests_per_second: 10  # Max 10 requests/second
        burst: 5                 # Allow bursts up to 5 requests
      max_retries: 3             # Retry transient errors 3 times
```

### Retry Behavior

The plugin automatically retries:
- **HTTP 429 (Too Many Requests)**: Waits for `Retry-After` header duration (capped at 300s), or uses exponential backoff if header absent
- **Transient errors (500, 502, 503, 504)**: Exponential backoff with formula `min(1.0 * 2^attempt, 60)`

After `max_retries` exhausted, raises error with HTTP status, URL, and attempt count.

---

## Wildcard Adapter Architecture

The REST API plugin uses a wildcard adapter that works with any Arrow-compatible engine. You don't need engine-specific adapters.

### How It Works

1. The `RestApiAdapter` registers with `target_engine_type = "*"` (wildcard)
2. When compiling a pipeline with a REST API catalog, the registry checks:
   - Is there an exact adapter for `(engine_type, rest_api)`? No
   - Is there a wildcard adapter for `("*", rest_api)`? Yes
   - Does the engine support Arrow input? Check `supported_catalog_types`
3. If the engine declares `"arrow"` support, the wildcard adapter is used

### Engine Compatibility

| Engine | Arrow Support | REST API Works? |
|--------|---------------|-----------------|
| DuckDB | ✅ | ✅ |
| Polars | ✅ | ✅ |
| PySpark | ✅ | ✅ |
| Databricks | ❌ | ❌ (use Unity catalog) |
| Postgres | ❌ | ❌ |


### Pushdown Capabilities

The wildcard adapter declares:
- `projection_pushdown` — Only fetches needed columns (if API supports field selection)
- `predicate_pushdown` — Translates filters to query parameters
- `limit_pushdown` — Stops pagination early when SQL LIMIT is reached

---

## Usage Examples

### Source Joint

Read data from a REST API endpoint:

=== "SQL"

    ```sql
    -- rivet:name: api_users
    -- rivet:type: source
    -- rivet:catalog: my_api
    -- rivet:table: users
    ```

=== "YAML"

    ```yaml
    name: api_users
    type: source
    catalog: my_api
    table: users
    ```

The `table` field references the endpoint name in your catalog configuration.

### Transform with Predicate Pushdown

```sql
-- rivet:name: active_users
-- rivet:type: sql
-- rivet:upstream: api_users

SELECT id, name, email
FROM api_users
WHERE status = 'active'
  AND created_after > '2024-01-01'
```

If `filter_params` declares `status` and `created_after`, these predicates are pushed to the API as query parameters. Otherwise, they're applied post-fetch.

### Sink Joint

Write data back to a REST API:

=== "SQL"

    ```sql
    -- rivet:name: write_users
    -- rivet:type: sink
    -- rivet:upstream: transformed_users
    -- rivet:catalog: my_api
    -- rivet:table: users
    -- rivet:write_strategy: append
    ```

=== "YAML"

    ```yaml
    name: write_users
    type: sink
    upstream: transformed_users
    catalog: my_api
    table: users
    write_strategy: append
    ```

- `append` strategy → POST requests
- `replace` strategy → PUT requests (or PATCH if `write_method: PATCH`)

### Batched Writes

```yaml
endpoints:
  users:
    path: /users
    batch_size: 10  # Send 10 rows per request
```

Single row → JSON object. Multiple rows → JSON array.

---

## Real-World Examples

### PokeAPI

```yaml
catalogs:
  - name: pokeapi
    type: rest_api
    options:
      base_url: https://pokeapi.co/api/v2
      timeout: 30
      endpoints:
        pokemon:
          path: /pokemon
          response_path: results
          pagination:
            strategy: offset
            limit: 20
            has_more_field: next
```


Joint:
```sql
-- rivet:name: pokemon
-- rivet:type: source
-- rivet:catalog: pokeapi
-- rivet:table: pokemon

-- rivet:name: first_150
-- rivet:type: sql
-- rivet:upstream: pokemon
SELECT name, url FROM pokemon LIMIT 150
```

The plugin fetches 150 records across 8 pages (20 per page), stopping early due to limit pushdown.

### GitHub API

```yaml
catalogs:
  - name: github
    type: rest_api
    options:
      base_url: https://api.github.com
      auth: bearer
      token: ${GITHUB_TOKEN}
      default_headers:
        Accept: application/vnd.github+json
        X-GitHub-Api-Version: "2022-11-28"
      endpoints:
        repos:
          path: /user/repos
          pagination:
            strategy: link_header
        issues:
          path: /repos/{owner}/{repo}/issues
          pagination:
            strategy: page_number
            page_size: 30
          filter_params:
            state: state
            labels: labels
```

### Stripe API

```yaml
catalogs:
  - name: stripe
    type: rest_api
    options:
      base_url: https://api.stripe.com/v1
      auth: bearer
      token: ${STRIPE_API_KEY}
      rate_limit:
        requests_per_second: 25
        burst: 10
      endpoints:
        customers:
          path: /customers
          response_path: data
          pagination:
            strategy: cursor
            cursor_field: has_more
            cursor_param: starting_after
          filter_params:
            email: email
            created: created
```

---

## Error Handling

The plugin provides clear error messages for common failure scenarios:

### Authentication Errors (HTTP 401/403)

```
Error: RVT-201 Authentication/authorization failed (HTTP 401)
for https://api.example.com/users using auth strategy 'bearer'

Remediation: Check your credentials and auth configuration.
```

### Endpoint Not Found (HTTP 404)

```
Error: RVT-501 Endpoint not found (HTTP 404):
https://api.example.com/invalid

Remediation: Verify the endpoint path exists in the API.
```

### JSON Parse Failure

```
Error: RVT-501 JSON parse error for https://api.example.com/users:
<!DOCTYPE html><html>...

Remediation: Check response_format and API response.
```

### Missing Response Path

```
Error: RVT-501 response_path 'data.users' not found in response.
Available keys: status, message

Remediation: Check the 'response_path' in your endpoint configuration.
```

### Network Errors

```
Error: RVT-501 Network error for https://api.example.com/users:
Connection refused

Remediation: Check network connectivity and the base_url.
```

---

## Pagination Strategies

### Offset/Limit


### Pagination Options

| Strategy | Options | Description |
|----------|---------|-------------|
| `offset` | `limit`, `offset_param`, `limit_param`, `has_more_field` | Offset/limit pagination with optional has_more indicator |
| `cursor` | `cursor_field`, `cursor_param` | Cursor-based pagination |
| `page_number` | `page_size`, `page_param`, `start_page`, `limit_param` | Page number pagination |
| `link_header` | - | RFC 8288 Link header following |
| `none` | - | Single request, no pagination |

---

## Deferred Materialization

The REST API adapter uses deferred materialization — HTTP requests only execute when data is actually needed.

### How It Works

1. **Compilation**: `RestApiAdapter.read_dispatch()` returns a deferred `Material` without making HTTP requests
2. **Execution**: When the engine needs data, it calls `to_arrow()` on the material
3. **Fetch**: The deferred ref executes HTTP requests, paginates, parses, and returns an Arrow table
4. **Caching**: The Arrow table is cached; subsequent `to_arrow()` calls return the cached result

This pattern:
- Avoids unnecessary HTTP requests during compilation
- Allows the optimizer to eliminate unused sources
- Matches the behavior of other Rivet adapters (DuckDB, Glue)

---

## Catalog Portability

Source joints work unchanged across catalog types. Only the catalog configuration changes:

=== "REST API"

    ```yaml
    # profiles.yaml
    catalogs:
      - name: orders_catalog
        type: rest_api
        options:
          base_url: https://api.example.com
          endpoints:
            orders:
              path: /orders
    ```

=== "DuckDB"

    ```yaml
    # profiles.yaml
    catalogs:
      - name: orders_catalog
        type: duckdb
        options:
          path: warehouse.duckdb
    ```

The source joint is identical:

```yaml
# joints/raw_orders.yaml
name: raw_orders
type: source
catalog: orders_catalog
table: orders
```

Switch between REST API and database by changing the catalog type in `profiles.yaml`. The joint doesn't change.

---

## Advanced Configuration

### Path Parameters

Use `{param}` syntax in endpoint paths:

```yaml
endpoints:
  user_orders:
    path: /users/{user_id}/orders
    params:
      user_id: "123"  # Default value
```

Override at query time via joint options or query parameters.

### Custom Headers Per Endpoint

```yaml
endpoints:
  special_endpoint:
    path: /special
    headers:
      X-Custom-Header: value
      Accept: application/vnd.api+json
```

Merged with catalog-level `default_headers`.

### Request Body Templates

For POST/PUT/PATCH endpoints:

```yaml
endpoints:
  create_user:
    path: /users
    method: POST
    body:
      template: true
```

The sink serializes Arrow rows to JSON and sends as request body.

### Explicit Schema

Skip schema inference by declaring the schema explicitly:

```yaml
endpoints:
  users:
    path: /users
    schema:
      id: int64
      name: utf8
      email: utf8
      created_at: timestamp[us]
```

---

## Known Limitations


- **Arrow-compatible engines only** — Databricks and Postgres engines cannot use REST API catalogs (they don't support Arrow input). Use Unity or native Postgres catalogs instead.
- **Best-effort predicate pushdown** — Only equality and comparison operators on declared `filter_params` columns are pushed down. Complex predicates (LIKE, IN, subqueries) are applied post-fetch.
- **No projection pushdown to API** — The plugin fetches all fields from the API response. Projection pushdown only affects which columns are kept in the Arrow table.
- **Schema inference from first page** — If later pages have different schemas, new columns are added with null backfill. Type mismatches are coerced.
- **Rate limiting is client-side** — The plugin enforces rate limits locally but cannot prevent other clients from exhausting API quotas.
- **No GraphQL support** — This plugin handles REST APIs only. GraphQL support will be a separate plugin.
- **Pagination stop conditions** — The plugin stops pagination when a page returns fewer records than the page size. APIs that always return full pages (even on the last page) may require explicit `has_more_field` configuration.

---

## Troubleshooting

### "Authentication failed (HTTP 401)"

Check:
- Credentials are correct and not expired
- Environment variables are set (`echo $REST_API_TOKEN`)
- Auth strategy matches API requirements (bearer vs basic vs api_key)

### "Only 20 records returned instead of 300"

Check:
- `response_path` is configured correctly to extract the record array
- Pagination `limit` is the page size (batch size), not the total record limit
- SQL `LIMIT` in your query specifies the total records you want

### "response_path 'data.users' not found"

Check:
- The API response structure matches your `response_path` configuration
- Use `rivet explore` to inspect the actual response structure
- Try without `response_path` first to see the raw response

### "Rate limit exceeded (HTTP 429)"

Configure rate limiting:
```yaml
rate_limit:
  requests_per_second: 10
  burst: 5
max_retries: 5
```

The plugin will automatically retry with backoff.

---

## See Also

- [Catalog Configuration](../reference/catalog-configuration.md)
- [Predicate Pushdown](../concepts/cross-group-predicate-pushdown.md)
- [Quality Checks](../guides/quality-checks.md)
- [Plugin Development](development.md)
