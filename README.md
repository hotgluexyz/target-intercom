# target-intercom

`target-intercom` is a Hotglue Singer target for Intercom passthrough writes.

Current scope is intentionally minimal:

- Only `FallbackSink` is used.
- Any incoming stream is written to `POST /{stream_name}`.
- Records are forwarded as-is (passthrough), so payload validity is determined by Intercom's API for that endpoint.

## Configuration

Required:

- `access_token`

Optional:

- `api_base_url` (default: `https://api.intercom.io`)
- `intercom_version` (default: `2.14`)
- `user_agent`

Example:

```json
{
  "access_token": "YOUR_INTERCOM_ACCESS_TOKEN"
}
```

## Stream behavior

There are no stream-specific sink classes right now. Every stream uses the same fallback behavior:

- Endpoint pattern: `POST /{stream_name}`
- Example:
  - `companies` -> `POST /companies`
  - `tags` -> `POST /tags`

### Example Singer input

From `.secrets/data.singer`:

```json
{"type":"SCHEMA","stream":"companies","schema":{"type":["object","null"],"properties":{"company_id":{"type":["string","null"]},"name":{"type":["string","null"]},"website":{"type":["string","null"]},"industry":{"type":["string","null"]},"size":{"type":["integer","null"]}}},"key_properties":[]}
{"type":"RECORD","stream":"companies","record":{"company_id":"1","name":"Acme Inc.","website":"","industry":"","size":1}}
{"type":"RECORD","stream":"companies","record":{"company_id":"2","name":"Other Acme Inc.","website":"","industry":"","size":2}}
{"type":"SCHEMA","stream":"tags","schema":{"type":["object","null"],"properties":{"name":{"type":["string","null"]},"companies":{"type":["array","null"],"items":{"type":"object"}}}},"key_properties":[]}
{"type":"RECORD","stream":"tags","record":{"name":"CreateTag"}}
{"type":"RECORD","stream":"tags","record":{"name":"CreateAndTagCompanies","companies":[{"company_id":"1"},{"company_id":"2"}]}}
{"type":"RECORD","stream":"tags","record":{"name":"CreateAndTagCompany","companies":[{"company_id":"1"}]}}
```

This results in requests to:

- `POST /companies` with the `companies` record payload
- `POST /tags` with the `tags` record payload

## Error behavior

The target maps Intercom responses into customer-actionable errors:

- `401`/`403` -> `Invalid Credentials`
- `400`/`404`/`409`/`422` -> `Invalid Payload`

In practice:

- `Invalid Credentials` means the token is invalid or lacks required Intercom permissions.
- `Invalid Payload` means the record body does not match what that Intercom endpoint accepts, or references missing entities.

## Development

### Setup Development Environment

Use Python `3.8`-`3.10` for this project

```bash
# Install dependencies
poetry install

# Run linting
poetry run ruff check .

# Test CLI
poetry run target-intercom --help
```