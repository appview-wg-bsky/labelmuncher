# labelmuncher

A service to ingest labels from atproto labelers into the Bluesky AppView.

## Environment Variables

### Required

- `BSKY_DB_POSTGRES_URL`: PostgreSQL connection URL for the AppView database
- `BSKY_LABELS_FROM_ISSUER_DIDS`: Comma-separated list of labeler DIDs to subscribe to

### Optional

- `BSKY_DB_POSTGRES_SCHEMA`: PostgreSQL schema to use (defaults to `bsky`)
- `DB_PATH`: Path to SQLite database file (defaults to `./muncher-state.sqlite`)
- `MOD_SERVICE_DID`: If you want to accept takedowns from a moderation service, set this to the DID
  of the service
- `BSKY_DATAPLANE_URLS`: Required with `MOD_SERVICE_DID`; comma-separated list of dataplane URLs
- `BSKY_DATAPLANE_HTTP_VERSION`: HTTP version for dataplane requests (defaults to `1.1`)
