# MCP Worker

## Launch Configuration

| Variable | Required | Default | Validation |
| --- | --- | --- | --- |
| `TOTAL_RECALL_MEMORY_DATABASE` | Yes | None | Non-blank database target |
| `TOTAL_RECALL_EMBEDDING_PROVIDER` | No | Disabled | Non-blank only when the model is also non-blank |
| `TOTAL_RECALL_EMBEDDING_MODEL` | No | Disabled | Non-blank only when the provider is also non-blank |
| `TOTAL_RECALL_MCP_EMBEDDING_TIMEOUT_MS` | No | `21000` | Integer `1..=30000`, validated only when embedding is enabled |
| `TOTAL_RECALL_MCP_MAX_LINE_BYTES` | No | `4194304` | Positive `usize` |
| `TOTAL_RECALL_MCP_CHANNEL_CAPACITY` | No | `32` | Positive `usize` |
| `TOTAL_RECALL_MCP_MAX_IN_FLIGHT` | No | `16` | Positive `usize` |
| `TOTAL_RECALL_MCP_CONTEXT_DEFAULT_LIMIT` | No | `10` | Positive count no greater than the maximum |
| `TOTAL_RECALL_MCP_CONTEXT_MAX_LIMIT` | No | `20` | Integer `1..=50` |
| `TOTAL_RECALL_MCP_CONTEXT_MAX_RESULT_BYTES` | No | `32768` | Positive byte bound large enough for `{"memories":[]}` |
| `TOTAL_RECALL_MCP_CONTEXT_TIMEOUT_MS` | No | `5000` | Integer `1..=30000` |
| `TOTAL_RECALL_MCP_CONTEXT_DATABASE_TIMEOUT_MS` | No | `3000` | Integer `1..=context timeout` |
| `III_URL` | No | iii SDK default | Existing managed iii convention |
| `III_WORKER_NAME` | No | SDK-managed name | Existing managed iii convention |
| `III_NAMESPACE` | No | iii SDK default | Existing managed iii convention |

The worker validates this configuration before it takes ownership of stdout. A
missing or blank `TOTAL_RECALL_MEMORY_DATABASE`, a malformed or zero configured
resource bound, a partial embedding configuration, or an invalid enabled
embedding timeout writes one content-free diagnostic to stderr and exits
unsuccessfully. Whitespace-only `III_URL` and `III_NAMESPACE` select
their iii SDK defaults. Only an absent or empty `III_WORKER_NAME` leaves the
SDK-managed name unset; a whitespace-only `III_WORKER_NAME` is retained.
Startup starts the iii background connection but does not await readiness or
probe the database. Connectivity failures occur only when a tool invokes the
backend.

`memory_context` is the sixth tool. Its optional `limit` is query-free and must
be within the configured maximum. The worker returns only type, title, content,
and concepts, with complete records capped by the configured structured-result
byte limit. Database and operation failures return fixed tool errors without
partial context.

Query embedding is disabled when `TOTAL_RECALL_EMBEDDING_PROVIDER` and
`TOTAL_RECALL_EMBEDDING_MODEL` are both absent, empty, or whitespace-only;
`TOTAL_RECALL_MCP_EMBEDDING_TIMEOUT_MS` is then ignored. It is enabled when both
are non-blank, and their values are used exactly as supplied. Exactly one
non-blank value is a startup error. Enabled deployments must use the provider
and model configured for stored-memory embeddings; startup does not probe the
embedding router.
