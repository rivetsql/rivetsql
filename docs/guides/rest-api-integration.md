# REST API Integration

This guide shows you how to integrate REST APIs into your Rivet pipelines. You'll learn how to configure endpoints, handle authentication, optimize with predicate pushdown, and troubleshoot common issues.

---

## Quick Start

Install the REST API plugin:

```bash
pip install 'rivetsql[rest]'
```

Add a REST API catalog to your `profiles.yaml`:

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
        endpoints:
          users:
            path: /users
            response_path: data
```

Create a source joint:

```sql
-- rivet:name: api_users
-- rivet:type: source
-- rivet:catalog: my_api
-- rivet:table: users
```

Run your pipeline:

```bash
export REST_API_TOKEN="your-token-here"
rivet run
```

---

## Understanding Endpoints as Tables

In Rivet, each REST API endpoint is modeled as a logical table. The endpoint name (e.g. `users`) becomes the table name you reference in your joints.

### Endpoint Configuration

```yaml
endpoints:
  users:              # Table name
    path: /users      # API endpoint path
    method: GET       # HTTP method
    response_path: data.users  # Where records are in the response
```

### Using in Joints

```sql
-- Reference the endpoint by its table name
SELECT * FROM users WHERE status = 'active'
```

The plugin automatically:
1. Resolves `users` to the `/users` endpoint
2. Makes HTTP GET request to `https://api.example.com/v1/users`
3. Extracts records from `data.users` in the response
4. Converts JSON to Arrow table
5. Returns data to your engine (DuckDB, Polars, etc.)

---

## Working with Pagination

Most APIs return data in pages. The REST API plugin handles pagination automatically.

### Choosing a Pagination Strategy

Identify your API's pagination method by checking its documentation:

| API Uses | Strategy | Config Example |
|----------|----------|----------------|
| `?offset=0&limit=100` | `offset` | `strategy: offset` |
| `?cursor=abc123` | `cursor` | `strategy: cursor` |
| `?page=1` | `page_number` | `strategy: page_number` |
| `Link: <url>; rel="next"` header | `link_header` | `strategy: link_header` |
| Single response, no pages | `none` | No pagination config |

### Offset/Limit Example

```yaml
endpoints:
  users:
    path: /users
    pagination:
      strategy: offset
      limit: 100              # Records per page
      offset_param: offset    # Query param name
      limit_param: limit      # Query param name
```

The plugin sends:
- `GET /users?offset=0&limit=100`
- `GET /users?offset=100&limit=100`
- `GET /users?offset=200&limit=100`
- Stops when a page returns < 100 records

### Cursor-Based Example

```yaml
endpoints:
  orders:
    path: /orders
    pagination:
      strategy: cursor
      cursor_field: next_cursor  # Field in response containing next cursor
      cursor_param: cursor       # Query param name
```

API response:
```json
{
  "data": [...],
  "next_cursor": "eyJpZCI6MTIzfQ=="
}
```

The plugin extracts `next_cursor` and sends it on the next request.

### Important: Pagination Limit vs SQL LIMIT

These are different concepts:

- **Pagination `limit`**: Page size (records per HTTP request)
- **SQL `LIMIT`**: Total records you want

Example:
```yaml
pagination:
  limit: 20  # Fetch 20 records per page
```

```sql
SELECT * FROM users LIMIT 300  -- Get 300 total records
```

The plugin fetches 15 pages (20 × 15 = 300) and stops.

---

## Authentication Patterns

### Environment Variables (Recommended)

Store credentials in environment variables, reference in config:

```yaml
catalogs:
  - name: my_api
    type: rest_api
    options:
      base_url: https://api.example.com
      auth: bearer
      token: ${REST_API_TOKEN}
```

```bash
export REST_API_TOKEN="sk-abc123..."
rivet run
```


### Multiple APIs with Different Auth

```yaml
catalogs:
  - name: github
    type: rest_api
    options:
      base_url: https://api.github.com
      auth: bearer
      token: ${GITHUB_TOKEN}

  - name: stripe
    type: rest_api
    options:
      base_url: https://api.stripe.com/v1
      auth: bearer
      token: ${STRIPE_API_KEY}

  - name: internal_api
    type: rest_api
    options:
      base_url: https://internal.company.com/api
      auth: basic
      username: ${INTERNAL_API_USER}
      password: ${INTERNAL_API_PASS}
```

### OAuth2 with Auto-Refresh

```yaml
catalogs:
  - name: oauth_api
    type: rest_api
    options:
      base_url: https://api.example.com
      auth: oauth2
      client_id: ${OAUTH_CLIENT_ID}
      client_secret: ${OAUTH_CLIENT_SECRET}
      token_url: https://auth.example.com/oauth/token
```

The plugin:
1. Exchanges credentials for access token on first request
2. Caches token for subsequent requests
3. Automatically refreshes when token expires

---

## Optimizing API Calls with Predicate Pushdown

Reduce data transfer and API load by pushing filters to the API.

### Step 1: Identify Supported Filters

Check your API documentation for query parameters:

```
GET /users?status=active&created_after=2024-01-01
```

### Step 2: Configure filter_params

Map SQL columns to API query parameters:

```yaml
endpoints:
  users:
    path: /users
    filter_params:
      status: status              # Column 'status' → param 'status'
      created_after: since        # Column 'created_after' → param 'since'
      email: email
```

### Step 3: Write SQL with Filters

```sql
SELECT id, name, email
FROM users
WHERE status = 'active'
  AND created_after > '2024-01-01'
  AND email LIKE '%@example.com'
```

The plugin:
- Pushes `status = 'active'` → `?status=active`
- Pushes `created_after > '2024-01-01'` → `?since=2024-01-01`
- Applies `email LIKE '%@example.com'` as residual filter (LIKE not supported for pushdown)

### Supported Operators

| SQL | Query Param |
|-----|-------------|
| `column = 'value'` | `?column=value` |
| `column < 100` | `?column_lt=100` |
| `column > 100` | `?column_gt=100` |
| `column <= 100` | `?column_lte=100` |
| `column >= 100` | `?column_gte=100` |

Unsupported operators (LIKE, IN, BETWEEN) become residual filters.

---

## Handling Nested JSON

APIs often return nested JSON structures. The plugin flattens them automatically.

### Example Response

```json
{
  "id": 1,
  "name": "Alice",
  "address": {
    "city": "NYC",
    "state": "NY",
    "coordinates": {
      "lat": 40.7128,
      "lon": -74.0060
    }
  }
}
```


### Flattened Columns

With `max_flatten_depth: 3` (default):

| Column | Type | Value |
|--------|------|-------|
| `id` | int64 | 1 |
| `name` | utf8 | "Alice" |
| `address.city` | utf8 | "NYC" |
| `address.state` | utf8 | "NY" |
| `address.coordinates.lat` | float64 | 40.7128 |
| `address.coordinates.lon` | float64 | -74.0060 |

### Querying Nested Fields

Use dot-separated column names in SQL:

```sql
SELECT
  name,
  "address.city" AS city,
  "address.coordinates.lat" AS latitude
FROM users
WHERE "address.state" = 'NY'
```

!!! tip "Quote nested column names"
    Use double quotes around dot-separated column names to avoid SQL parser confusion.

### Controlling Flatten Depth

```yaml
catalogs:
  - name: my_api
    type: rest_api
    options:
      base_url: https://api.example.com
      max_flatten_depth: 2  # Only flatten 2 levels
```

Objects beyond depth 2 are JSON-serialized as strings.

### Arrays in Responses

Arrays are always JSON-serialized as strings:

```json
{"tags": ["python", "data", "api"]}
```

Becomes:
- `tags` (large_utf8): `'["python", "data", "api"]'`

Parse in SQL:
```sql
SELECT
  name,
  json_extract_string(tags, '$[0]') AS first_tag
FROM users
```

---

## Writing Data to APIs

Use sink joints to POST/PUT data to REST endpoints.

### Append Strategy (POST)

```yaml
endpoints:
  users:
    path: /users
    method: POST
```

```sql
-- rivet:name: write_new_users
-- rivet:type: sink
-- rivet:upstream: transformed_users
-- rivet:catalog: my_api
-- rivet:table: users
-- rivet:write_strategy: append
```

Each row becomes a POST request with JSON body:
```json
{"id": 1, "name": "Alice", "email": "alice@example.com"}
```

### Replace Strategy (PUT)

```yaml
endpoints:
  users:
    path: /users/{id}
    method: PUT
```

```sql
-- rivet:write_strategy: replace
```

Each row becomes a PUT request.

### Batched Writes

Send multiple rows per request:

```yaml
endpoints:
  users:
    path: /users/batch
    batch_size: 10  # 10 rows per request
```

Request body becomes a JSON array:
```json
[
  {"id": 1, "name": "Alice"},
  {"id": 2, "name": "Bob"},
  ...
]
```


### PATCH Method Override

```yaml
endpoints:
  users:
    path: /users/{id}
    write_method: PATCH
```

Uses PATCH instead of PUT for `replace` strategy.

---

## Rate Limiting Best Practices

### Configure Based on API Limits

Check your API's rate limit documentation:

```yaml
# API allows 100 requests/minute
rate_limit:
  requests_per_second: 1.67  # 100/60
  burst: 10                  # Allow short bursts
```

### Handling 429 Responses

The plugin automatically:
1. Reads `Retry-After` header
2. Waits the specified duration (capped at 300s)
3. Retries the request
4. Falls back to exponential backoff if header absent

### Retry Configuration

```yaml
catalogs:
  - name: my_api
    type: rest_api
    options:
      max_retries: 5  # Retry transient errors up to 5 times
      rate_limit:
        requests_per_second: 10
```

Retries apply to:
- HTTP 429 (Too Many Requests)
- HTTP 500, 502, 503, 504 (transient server errors)

---

## Schema Inference

The plugin infers Arrow schemas from API responses automatically.

### How It Works

1. Fetches first page of data
2. Samples records to infer types
3. Applies schema to all subsequent pages
4. Handles schema evolution (new columns, type mismatches)

### Type Inference Rules

```json
{
  "id": 123,              // → int64
  "name": "Alice",        // → utf8
  "score": 95.5,          // → float64
  "active": true,         // → bool
  "metadata": null,       // → utf8 (nullable)
  "tags": ["a", "b"],     // → large_utf8 (JSON-serialized)
  "nested": {"x": 1}      // → Flattened or large_utf8 (depends on depth)
}
```

### Explicit Schema

Skip inference by declaring the schema:

```yaml
endpoints:
  users:
    path: /users
    schema:
      id: int64
      name: utf8
      email: utf8
      created_at: timestamp[us]
      score: float64
```

Benefits:
- No sample request during schema introspection
- Consistent types across runs
- Control over timestamp precision

---

## Common Patterns

### Pattern 1: Paginated API → Transform → Database

```yaml
# profiles.yaml
default:
  engines:
    - name: local
      type: duckdb
      catalogs: [api, warehouse]
  catalogs:
    - name: api
      type: rest_api
      options:
        base_url: https://api.example.com
        auth: bearer
        token: ${API_TOKEN}
        endpoints:
          events:
            path: /events
            response_path: data
            pagination:
              strategy: offset
              limit: 1000
    - name: warehouse
      type: duckdb
      options:
        path: warehouse.duckdb
```


Pipeline:
```sql
-- joints/api_events.sql
-- rivet:name: api_events
-- rivet:type: source
-- rivet:catalog: api
-- rivet:table: events

-- joints/clean_events.sql
-- rivet:name: clean_events
-- rivet:type: sql
-- rivet:upstream: api_events
SELECT
  id,
  event_type,
  user_id,
  CAST(timestamp AS TIMESTAMP) AS event_time
FROM api_events
WHERE event_type IN ('login', 'purchase')

-- joints/write_events.sql
-- rivet:name: write_events
-- rivet:type: sink
-- rivet:upstream: clean_events
-- rivet:catalog: warehouse
-- rivet:table: events
-- rivet:write_strategy: append
```

### Pattern 2: Multiple APIs → Join → Output

```yaml
catalogs:
  - name: users_api
    type: rest_api
    options:
      base_url: https://users.example.com
      endpoints:
        users:
          path: /users

  - name: orders_api
    type: rest_api
    options:
      base_url: https://orders.example.com
      endpoints:
        orders:
          path: /orders
```

Pipeline:
```sql
-- rivet:name: users
-- rivet:type: source
-- rivet:catalog: users_api
-- rivet:table: users

-- rivet:name: orders
-- rivet:type: source
-- rivet:catalog: orders_api
-- rivet:table: orders

-- rivet:name: user_orders
-- rivet:type: sql
-- rivet:upstream: [users, orders]
SELECT
  u.name,
  u.email,
  o.order_id,
  o.amount
FROM users u
JOIN orders o ON u.id = o.user_id
```

### Pattern 3: Database → Transform → API Sink

```yaml
catalogs:
  - name: warehouse
    type: duckdb
    options:
      path: warehouse.duckdb

  - name: webhook_api
    type: rest_api
    options:
      base_url: https://webhooks.example.com
      auth: api_key
      api_key_name: X-API-Key
      api_key_value: ${WEBHOOK_KEY}
      endpoints:
        events:
          path: /events
          method: POST
          batch_size: 50
```

Pipeline:
```sql
-- Read from database
-- rivet:name: high_value_orders
-- rivet:type: source
-- rivet:catalog: warehouse
-- rivet:table: orders

-- Transform
-- rivet:name: order_events
-- rivet:type: sql
-- rivet:upstream: high_value_orders
SELECT
  order_id,
  customer_id,
  amount,
  'order_completed' AS event_type
FROM high_value_orders
WHERE amount > 1000

-- Write to API
-- rivet:name: send_events
-- rivet:type: sink
-- rivet:upstream: order_events
-- rivet:catalog: webhook_api
-- rivet:table: events
-- rivet:write_strategy: append
```


---

## Debugging and Troubleshooting

### Inspect API Responses

Use `rivet explore` to see raw API responses:

```bash
rivet explore --profile default
```

In the REPL:
```sql
> SELECT * FROM my_api.users LIMIT 5;
```

This shows you the actual column names and types after flattening.

### Check Pagination Behavior

Add a small LIMIT to verify pagination works:

```sql
SELECT * FROM users LIMIT 50;
```

If your pagination `limit` is 20, you should see 3 HTTP requests (20 + 20 + 10).

### Verify Predicate Pushdown

Check the execution plan to see which predicates were pushed:

```bash
rivet compile --verbose
```

Look for `AdapterPushdownResult` with `query_params` and `residual` in the output.

### Common Issues

#### Issue: "Only getting first page of results"

**Cause**: `response_path` not configured correctly.

**Solution**: The paginator needs to extract records to determine page size. If `response_path` is wrong, it can't find the records and stops after the first page.

```yaml
# Wrong: response_path not set, but records are nested
endpoints:
  users:
    path: /users
    # Missing: response_path: results

# Correct
endpoints:
  users:
    path: /users
    response_path: results
```

#### Issue: "Pagination limit of 20 limiting total results"

**Cause**: Confusion between pagination `limit` (page size) and SQL `LIMIT` (total records).

**Solution**:
- Pagination `limit`: How many records per HTTP request
- SQL `LIMIT`: How many total records you want

```yaml
pagination:
  limit: 20  # Page size
```

```sql
SELECT * FROM users LIMIT 300  -- Total records
```

#### Issue: "HTTP 401 authentication failed"

**Cause**: Invalid or expired credentials.

**Solution**:
1. Verify environment variable is set: `echo $REST_API_TOKEN`
2. Check token hasn't expired
3. Verify auth strategy matches API requirements
4. Test with curl: `curl -H "Authorization: Bearer $REST_API_TOKEN" https://api.example.com/users`

#### Issue: "response_path 'data.users' not found"

**Cause**: API response structure doesn't match configuration.

**Solution**:
1. Check actual API response structure
2. Update `response_path` to match
3. If response is a top-level array, omit `response_path`

---

## Performance Tips

### 1. Use Predicate Pushdown

Push filters to the API instead of fetching everything:

```yaml
filter_params:
  status: status
  created_after: since
```

```sql
-- Good: Filters pushed to API
SELECT * FROM users WHERE status = 'active'

-- Bad: Fetches all users, filters locally
SELECT * FROM users
```


### 2. Use SQL LIMIT for Large Datasets

The plugin stops pagination when it has enough records:

```sql
-- Fetches only 100 records, stops pagination early
SELECT * FROM users LIMIT 100
```

### 3. Increase Pagination Page Size

Fewer HTTP requests = faster execution:

```yaml
pagination:
  strategy: offset
  limit: 1000  # Larger pages = fewer requests
```

Balance between:
- Larger pages: Fewer HTTP requests, faster overall
- Smaller pages: Lower memory usage, better for rate-limited APIs

### 4. Configure Rate Limits Appropriately

Match your API's actual limits:

```yaml
# API allows 100 req/min
rate_limit:
  requests_per_second: 1.67  # 100/60
  burst: 10
```

Too conservative = slower. Too aggressive = HTTP 429 errors.

### 5. Reduce Flatten Depth for Simple Data

```yaml
max_flatten_depth: 1  # Only flatten one level
```

Less flattening = faster parsing, but nested objects become JSON strings.

---

## Testing REST API Pipelines

### Use Fixture Data for Tests

Don't hit real APIs in tests. Use `rivet test` with fixture data:

```yaml
# tests/test_user_pipeline.yaml
name: test_user_pipeline
fixtures:
  api_users:
    - id: 1
      name: Alice
      status: active
    - id: 2
      name: Bob
      status: inactive
expected:
  active_users:
    - id: 1
      name: Alice
```

### Mock HTTP Responses in Integration Tests

For integration testing, use mocked HTTP responses:

```python
import responses

@responses.activate
def test_rest_api_source():
    responses.add(
        responses.GET,
        "https://api.example.com/users",
        json={"data": [{"id": 1, "name": "Alice"}]},
        status=200,
    )
    # Test your pipeline
```

---

## Migration Guide

### From Manual Python Joints

Before (manual HTTP handling):
```python
# joints/fetch_users.py
# rivet:name: fetch_users
# rivet:type: python

import requests
import pyarrow as pa

def transform() -> pa.Table:
    resp = requests.get("https://api.example.com/users")
    data = resp.json()["data"]
    return pa.Table.from_pylist(data)
```

After (declarative REST API catalog):
```yaml
# profiles.yaml
catalogs:
  - name: my_api
    type: rest_api
    options:
      base_url: https://api.example.com
      endpoints:
        users:
          path: /users
          response_path: data
```

```sql
-- joints/fetch_users.sql
-- rivet:name: fetch_users
-- rivet:type: source
-- rivet:catalog: my_api
-- rivet:table: users
```

Benefits:
- No Python code to maintain
- Automatic pagination
- Predicate pushdown
- Rate limiting
- Error handling


### From Hardcoded URLs to Configurable Endpoints

Before:
```python
# Hardcoded in Python joint
url = "https://api.example.com/users?status=active"
```

After:
```yaml
# Configurable in profiles.yaml
endpoints:
  users:
    path: /users
    filter_params:
      status: status
```

```sql
-- Declarative in SQL
SELECT * FROM users WHERE status = 'active'
```

---

## Reference

### Complete Configuration Example

```yaml
catalogs:
  - name: my_api
    type: rest_api
    options:
      # Required
      base_url: https://api.example.com/v1

      # Authentication
      auth: bearer
      token: ${REST_API_TOKEN}

      # Session config
      default_headers:
        Accept: application/json
        User-Agent: Rivet/0.1.0
      timeout: 30

      # Response handling
      response_format: json
      max_flatten_depth: 3

      # Rate limiting
      rate_limit:
        requests_per_second: 10
        burst: 5
      max_retries: 3

      # Endpoints
      endpoints:
        users:
          path: /users
          method: GET
          params:
            sort: created_at
          headers:
            X-Custom: value
          response_path: data.users
          pagination:
            strategy: offset
            limit: 100
            offset_param: offset
            limit_param: limit
          filter_params:
            status: status
            created_after: since
            email: email
          schema:
            id: int64
            name: utf8
            email: utf8
            created_at: timestamp[us]

        orders:
          path: /orders
          method: GET
          response_path: data
          pagination:
            strategy: cursor
            cursor_field: next_cursor
            cursor_param: cursor
          filter_params:
            customer_id: customer_id
            status: status

        create_order:
          path: /orders
          method: POST
          batch_size: 10
```

### Pagination Strategy Reference

| Strategy | Required Config | Optional Config | Stop Condition |
|----------|----------------|-----------------|----------------|
| `offset` | `limit` | `offset_param`, `limit_param`, `has_more_field` | Page returns < `limit` records |
| `cursor` | `cursor_field`, `cursor_param` | - | Cursor is null/absent |
| `page_number` | `page_size` | `page_param`, `start_page`, `limit_param` | Page returns < `page_size` records |
| `link_header` | - | - | No `next` link in header |
| `none` | - | - | Single request |

---

## See Also

- [REST API Plugin Reference](../plugins/rest.md)
- [Predicate Pushdown](../concepts/cross-group-predicate-pushdown.md)
- [Quality Checks](quality-checks.md)
- [Testing Pipelines](testing.md)
