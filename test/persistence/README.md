# Redis persistence regression tests

This battery reproduces the September 2026 incident: a live RDB beside an older,
loadable AOF containing zero or obsolete keys. It also protects the opposite
case, where an active incremental AOF contains writes missing from the RDB.

## Docker / CI

Build the actual wrapper image, then run:

```sh
docker build -f redis-sentinel/Dockerfile --build-arg REDIS_VERSION=8.2 -t redis-sentinel-e2e:local .
SEED_IMAGE=redis:8.2.1 python3 test/persistence/run.py
```

The CI matrix checks 7.2, 7.4 and 8.2. Each case owns and cleans up its own named
volumes and containers. Every migration verifies 57,401 deterministic values,
collection types in databases 5 and 15, and live/expired TTLs. It repeats those
checks after graceful shutdown, SIGKILL, and another graceful shutdown. Writes
made after migration are checked too, after the `appendfsync everysec` boundary.
This does not claim durability of writes that Redis has not fsynced.

Eight cases cover empty/nonempty abandoned AOF at `/data` and the nested
`/bitnami/redis/data` layout, a valid incremental AOF with missing/corrupt ownership
markers, a dangling AOF entry, and a broken manifest path. The invalid-file cases
must refuse boot without changing the original persistence files (SHA-256).
The workflow additionally runs disk/rewrite failures, conversion crash windows,
replication, concurrent writes, idle restarts, and Sentinel failover from
`test/e2e.sh`. The existing full E2E job also runs on this test branch.

The negative control runs the incident case on the exact pre-fix build
`686489d`. It must fail specifically on missing dataset values; a pull, boot or
fixture failure is not accepted as proof of regression detection.

`PERSISTENCE_CASES` selects comma-separated case names. `IMAGE`, `SEED_IMAGE`,
`PERSISTENCE_KEYS` and `ARTIFACTS` can override defaults. Results and container
stdout/stderr are uploaded by CI, including failure tracebacks.

## Real Railway volume and image swaps

This creates a new disposable project in the explicitly selected workspace,
attaches a `/bitnami` volume, and deploys the real legacy image. It disables AOF,
writes and verifies the dataset, and waits eleven minutes before saving the RDB
so it exercises the production timestamp rule without editing timestamps.
Then it swaps to the pinned target, verifies all values across two redeploys,
reverts to the old image, writes new data, waits again, and re-patches. The final
redeploy must preserve both the original dataset and the foreign image's write.

```sh
cd test/persistence/railway
npm ci
RAILWAY_API_TOKEN=... RAILWAY_WORKSPACE_ID=... \
  TARGET_IMAGE=ghcr.io/railwayapp-templates/redis-ha/redis-sentinel:8.2-030c62c npm test
```

`RAILWAY_ADMIN_TOKEN_FILE` may replace the token environment variable. Tokens and
Redis passwords are never written to the report. This uses real resources and
takes about thirty minutes. It deletes only the project it created in `finally`,
after saving deployment logs and results. A hard process termination can prevent
cleanup: the project ID is persisted immediately in `results.json`, and any
cleanup failure is explicit. No fleet settings or existing projects are changed.
