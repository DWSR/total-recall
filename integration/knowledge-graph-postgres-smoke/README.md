# Knowledge Graph PostgreSQL Smoke Fixture

This task-local fixture starts an isolated PostgreSQL 17 or 18 cluster and runs
the schema, migration-security, deferred-invariant rollback, graph mutation
lifecycle, concurrent mutation/lock-order, deterministic graph discovery and
traversal, and clean rollback verifiers against a fresh UTF-8 database.

Build and run the PostgreSQL 17 schema verifier:

```sh
postgresql17="$(nix build --no-link --print-out-paths .#postgresql17-search)"
bash integration/knowledge-graph-postgres-smoke/scripts/setup.sh \
  "$postgresql17/bin" 17 statement-capture -- \
  bash integration/knowledge-graph-postgres-smoke/scripts/verify-schema.sh
```

Build and run the PostgreSQL 18 schema verifier:

```sh
postgresql18="$(nix build --no-link --print-out-paths .#postgresql18-search)"
bash integration/knowledge-graph-postgres-smoke/scripts/setup.sh \
  "$postgresql18/bin" 18 row-change-publishing-disabled -- \
  bash integration/knowledge-graph-postgres-smoke/scripts/verify-schema.sh
```

Build and run the PostgreSQL 17 mutation and concurrency verifier:

```sh
postgresql17="$(nix build --no-link --print-out-paths .#postgresql17-search)"
bash integration/knowledge-graph-postgres-smoke/scripts/setup.sh \
  "$postgresql17/bin" 17 statement-capture -- \
  bash integration/knowledge-graph-postgres-smoke/scripts/verify-mutations.sh
```

Build and run the PostgreSQL 17 traversal verifier:

```sh
postgresql17="$(nix build --no-link --print-out-paths .#postgresql17-search)"
bash integration/knowledge-graph-postgres-smoke/scripts/setup.sh \
  "$postgresql17/bin" 17 statement-capture -- \
  bash integration/knowledge-graph-postgres-smoke/scripts/verify-traversal.sh
```

Build and run the PostgreSQL 18 traversal verifier:

```sh
postgresql18="$(nix build --no-link --print-out-paths .#postgresql18-search)"
bash integration/knowledge-graph-postgres-smoke/scripts/setup.sh \
  "$postgresql18/bin" 18 row-change-publishing-disabled -- \
  bash integration/knowledge-graph-postgres-smoke/scripts/verify-traversal.sh
```

Build and run the PostgreSQL 18 mutation and concurrency verifier:

```sh
postgresql18="$(nix build --no-link --print-out-paths .#postgresql18-search)"
bash integration/knowledge-graph-postgres-smoke/scripts/setup.sh \
  "$postgresql18/bin" 18 row-change-publishing-disabled -- \
  bash integration/knowledge-graph-postgres-smoke/scripts/verify-mutations.sh
```

The setup command requires one worker prerequisite mode: `statement-capture` or
`row-change-publishing-disabled`. This fixture runs direct SQL and does not
start or configure an iii database worker. The mode declares the database-worker
deployment prerequisite for the logical target: configure statement capture or
disable row-change publishing to prevent row-key leakage. It is not a PostgreSQL
GUC, and the fixture does not inspect or change worker deployment state.

The timeout contract is `ADAPTER_QUERY_TIMEOUT_SECONDS = 10`,
`POSTGRES_STATEMENT_TIMEOUT_SECONDS = 12`, and
`III_INVOCATION_TIMEOUT_SECONDS = 15`. Only the 12-second value is a PostgreSQL
setting: setup applies it with
`ALTER ROLE knowledge_graph_application SET statement_timeout = '12s'`, and the
verifier reads it from an application-role connection. The adapter enforces the
10-second query timeout, and the iii invocation contract uses a 15-second
invocation timeout; PostgreSQL does not enforce the invocation timeout. The
package's timeout test and diagnostic keep these values explicit.

The temporary cluster uses a short, private `/tmp` root to fit Unix-domain
socket pathname limits. It listens only on a private Unix socket and sets
`listen_addresses` to the empty string. Its database uses UTF-8 and verifies an
8,192-byte PostgreSQL page size. A limited fixture bootstrap role is separate
from the `NOLOGIN` migration owner and the login-capable, non-superuser
`knowledge_graph_application` role. The bootstrap role can only connect and
`SET ROLE` to the migration owner; the migration creates the graph schema and
supplies the application's graph grants. Setup adds no graph grants to the
application role. The `initdb` superuser is setup-only and becomes `NOLOGIN`
before the verifier runs.

`verify-schema.sh` first applies the migration in a transaction, inserts a
concept without a preferred alias, forces deferred constraints, and confirms
the failed transaction left no schema. It then applies the migration cleanly
and checks the exact tables, columns, seeded relations, constraints, foreign
keys, indexes, routines, deferred triggers, object owners, fixed search paths,
and application/PUBLIC privileges. Permission probes run as the application
role, and the verifier finishes through `rollback.sh`, which drops only the
schema as its migration owner and confirms absence. `verify-fixture.sh` remains
the smaller migration-application and rollback smoke check. The setup trap stops
the server, confirms it is stopped, and removes only its fresh private
temporary root while preserving the verifier's exit status.

`verify-mutations.sh` applies the migration as
`knowledge_graph_migration_owner`, executes every lifecycle mutation through
the granted routines as `knowledge_graph_application`, and checks rejected
mutations against before/after snapshots of every graph table. It finishes by
using `rollback.sh` to drop only the graph schema and confirm its absence.
Successful output contains fixed, content-safe diagnostics.

The mutation verifier also runs repeated two-session races for identical and
overlapping concept/assertion creates, revision updates, alias and semantic
cross-swaps, source/delete interactions, evidence additions/removals, evidence
cap contention, and last-evidence orphan protection. For each race, one
`knowledge_graph_application` session executes its mutation and keeps the
transaction open while a second application session executes the competing
mutation. Before releasing the owner, the verifier confirms by `PGAPPNAME` and
`pg_stat_activity` that the contender is actively waiting on an advisory lock
held by the owner, with `pg_locks.objsubid = 1` for the migration's single-key
locks. It also checks that the contender's already-granted application lock
keys precede its waiting key in numeric order. For alias and semantic
cross-swaps, the expected first shared key is calculated with the migration's
restricted lock-key helpers through the fixture bootstrap's
`SET ROLE knowledge_graph_migration_owner` path, and the contender must wait on
that exact key. All mutation sessions remain
`knowledge_graph_application`. Owner and contender roles alternate across
repetitions, including cross-swaps with different lock sets.
Each wait has a bounded observation window, each PostgreSQL session is subject
to the fixture's 12-second statement timeout, and every race checks exact typed
outcomes plus canonical final state. Cross-swap rejections compare full graph
snapshots, and database diagnostics remain private and are reported only as
fixed verifier failures.

`verify-traversal.sh` applies the migration as the migration owner, seeds
concepts, relations, sources, mentions, and evidence through the granted graph
routines as `knowledge_graph_application`, then executes the copied static
neighbor, related-source, and path queries directly as that application role.
It checks all nine relation types, outgoing/incoming/either neighbor behavior,
symmetric reversal, relation filters, unique either-direction rows, typed
evidence projection, related-source roles, deterministic ordering, limits, and
empty results. Its path fixture branches and cycles; assertions cover simple
paths, shortest native-UUID ordering, orientations, full evidence, depth and
result limits, and the exact work-sentinel boundary at seven versus eight
generated states.

The verifier uses 2,048-byte source keys to make complete neighbor and path
payloads exceed 4 MiB, then requires the exact `result_too_large` marker with no
result field. For timeout behavior, the migration-owner session holds an
exclusive lock on the assertion table while the application role executes the
static path query. PostgreSQL cancels it at the fixture's exact 12-second
`statement_timeout`; the verifier requires a query error and empty stdout. It
also checks the 10-second adapter query and 15-second iii invocation timeout
values without treating them as PostgreSQL settings. Successful runs finish by
rolling back the graph schema and print only fixed, content-safe diagnostics.

Run the package contract test with the Git-backed development shell:

```sh
nix develop --command cargo test --locked --package knowledge-graph-postgres-smoke
```

Successful fixture runs print only fixed, content-safe role, timeout, migration,
rollback, and teardown diagnostics.
