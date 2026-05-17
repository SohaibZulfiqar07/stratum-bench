# MiniPoolMonitor Context

## Project Snapshot

MiniPoolMonitor is a small-scale .NET 8 mining pool monitor inspired by Miningcore (github.com/oliverw/miningcore). Miningcore is a high-performance C#/.NET mining pool server with asynchronous Stratum TCP handling, adaptive share difficulty, native C/C++ proof-of-work validation, PostgreSQL persistence, payment engines, operational controls, and a REST API on port 4000.

MiniPoolMonitor keeps the shape of the high-throughput ingestion path while staying intentionally small: accept miner TCP connections, receive share submissions, process them through an in-memory pipeline, maintain live per-miner statistics, persist shares in SQLite batches, and expose a minimal HTTP JSON API.

## Target

- Runtime: .NET 8.
- Main project: `MiniPoolMonitor.Core`.
- Main process type: Worker Service / hosted background services.
- Stratum TCP port: `3333`.
- HTTP API port: `5000`.
- Persistence: SQLite through `Microsoft.Data.Sqlite`.
- Logging: Serilog console logging.
- Future companion app: `MinerSimulator`, a console app that starts five local miner clients and sends share traffic to the TCP listener.
- Future deployment support: Docker.

## Core Goals

- Run a Stratum-style TCP listener on port `3333`.
- Use asynchronous socket I/O for miner connections.
- Use `System.IO.Pipelines` for connection reads and writes.
- Use `System.Threading.Channels` for the share-processing queue.
- Persist shares to SQLite in batches, flushing every 2 seconds and during shutdown.
- Avoid per-share database writes on the hot path.
- Track live per-miner stats with `ConcurrentDictionary` and `Interlocked`.
- Expose a minimal HTTP API on port `5000` returning JSON live stats.
- Keep implementation readable enough to serve as a learning-oriented miniature of a pool ingestion system.

## Initial Non-Goals

- No native crypto or C/C++ proof-of-work libraries.
- No production payment engine.
- No wallet or blockchain daemon integration.
- No PostgreSQL dependency.
- No multi-coin pool cluster.
- No full Miningcore configuration model.
- No WebSocket event stream in the initial version.
- No production-grade banning, DDoS controls, or payout accounting.

## Architecture Plan

The service should be decomposed into hosted services and small internal components:

- `StratumListenerService`: owns the TCP listener on port `3333`, accepts connections, and creates miner sessions.
- `MinerSession`: owns one miner connection, reads newline-delimited JSON frames through pipelines, writes responses, and forwards share submissions.
- `SharePipeline`: wraps a bounded channel for accepted share submissions and applies backpressure when processing falls behind.
- `ShareProcessor`: consumes share submissions, performs lightweight validation, updates stats, and forwards accepted shares to persistence.
- `StatsRegistry`: stores live counters keyed by miner id using `ConcurrentDictionary`; individual counters should be mutated with `Interlocked`.
- `ShareBatchWriter`: buffers accepted shares and writes them to SQLite every 2 seconds, also flushing on graceful shutdown.
- `HttpStatsApi`: hosts minimal HTTP endpoints on port `5000`.
- `DatabaseInitializer`: creates SQLite tables and indexes at startup.
- `MinerSimulator`: future console app for local load simulation with five miner clients.

## Suggested Data Shapes

`ShareSubmission` should likely include:

- Miner id / worker name.
- Connection id.
- Submitted job id.
- Nonce or share token.
- Difficulty.
- Submission timestamp in UTC.
- Raw request id if useful for correlation.

SQLite `shares` table should likely include:

- `id` integer primary key.
- `miner_id` text not null.
- `connection_id` text not null.
- `job_id` text.
- `nonce` text.
- `difficulty` real not null.
- `accepted` integer not null.
- `submitted_at_utc` text not null.
- `created_at_utc` text not null.

Live stats snapshots should likely include:

- Total connections.
- Active connections.
- Total shares.
- Accepted shares.
- Rejected shares.
- Shares per second over a short moving window.
- Per-miner totals and last seen timestamp.

## HTTP API Draft

Keep the first API intentionally small:

- `GET /health`: returns service health and current UTC time.
- `GET /stats`: returns global live counters.
- `GET /miners`: returns all miner live snapshots.
- `GET /miners/{minerId}`: returns one miner snapshot or `404`.

All responses should be JSON. Avoid adding a frontend until explicitly requested.

## Stratum Protocol Approach

The initial TCP protocol can be a practical Stratum-style subset:

- Line-delimited JSON messages over TCP.
- Accept subscription/login-style messages if needed for state.
- Accept share submission messages and enqueue them through the channel.
- Return JSON responses with request ids when supplied.
- Treat malformed messages as rejected submissions or protocol errors without crashing the session.

The goal is to exercise the ingestion architecture, not to implement a complete coin-specific Stratum dialect.

## Performance Rules

- Do not write to SQLite per share.
- Do not put coarse locks around the share hot path.
- Prefer bounded channels to unbounded memory growth.
- Prefer async socket and stream APIs end to end.
- Use cancellation tokens in all long-running loops.
- Use UTC timestamps.
- Keep per-connection allocations modest.
- Avoid `Task.Run` around naturally asynchronous I/O.
- Flush persistence buffers on shutdown.

## Coding Conventions

- Use nullable reference types.
- Prefer small sealed classes for infrastructure components unless inheritance is needed.
- Use records for immutable messages and snapshots.
- Keep options/configuration classes explicit and bindable.
- Keep public contracts simple and JSON-friendly.
- Add tests when behavior becomes nontrivial, especially stats aggregation, channel flow, and SQLite batch flushing.

## Planned Folder Shape

```text
MiniPoolMonitor/
  MiniPoolMonitor.Core/
    MiniPoolMonitor.Core.csproj
    Program.cs
    Worker.cs
```

Future additions may include:

```text
MiniPoolMonitor/
  MiniPoolMonitor.Core/
    Configuration/
    Stratum/
    Shares/
    Stats/
    Persistence/
    Api/
  MinerSimulator/
  docker-compose.yml
  Dockerfile
```

## Implementation Order

1. Scaffold the Worker Service project and shared context.
2. Add configuration objects for ports, SQLite path, channel capacity, and flush interval.
3. Add models for share submissions, persisted shares, and stats snapshots.
4. Implement `StatsRegistry`.
5. Implement the channel-based share pipeline.
6. Implement SQLite initialization and batch flushing.
7. Implement the TCP listener and miner sessions.
8. Add the minimal HTTP API.
9. Add `MinerSimulator` for five local miner clients.
10. Add Docker support.

## Current State

The repository starts with a context file and an empty .NET 8 Worker Service scaffold. No Stratum listener, share pipeline, persistence, HTTP API, simulator, or Docker files have been implemented yet.
