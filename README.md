# MiniPoolMonitor

MiniPoolMonitor is a lightweight .NET 8 mining pool share processor built as a small, readable reference project. It focuses on the hot path of a mining pool: accepting newline-delimited JSON-RPC share submissions over TCP, moving them through an in-memory pipeline, tracking live counters, and persisting accepted shares efficiently to SQLite.

## Architecture

The data flow is intentionally simple:

`Stratum TCP listener (port 3333) -> Channel pipeline -> batch SQLite writes -> stats API`

- `StratumListener` accepts miner TCP connections and reads newline-delimited JSON-RPC messages with `System.IO.Pipelines`.
- Raw share payloads are pushed into a `Channel<ReadOnlyMemory<byte>>`.
- `ShareWorker` consumes the channel, parses miner and difficulty fields with `Utf8JsonReader`, and increments live counters.
- `MinerStats` tracks per-miner share totals with `ConcurrentDictionary` and `Interlocked`.
- `ShareBatchWriter` buffers accepted shares and flushes them to SQLite every 2 seconds inside a single transaction.
- A minimal HTTP endpoint exposes live stats on port `5050`.

## Run with `dotnet`

Start the monitor:

```bash
dotnet run --project MiniPoolMonitor/MiniPoolMonitor.Core
```

This opens:

- Stratum TCP listener: `localhost:3333`
- Stats API: `http://localhost:5050/stats`

Start the traffic generator in another terminal:

```bash
dotnet run --project MiniPoolMonitor/MockMiner
```

`MockMiner` connects to `localhost:3333` and sends 10 fake `mining.submit` messages per second using five rotating miner names and randomized difficulty values.

## Run with Docker

Build and run with Compose:

```bash
docker compose up --build
```

This publishes:

- `3333:3333` for TCP share input
- `5000:5000` in the current container config

If you want the container API port to match the current app binding, update the compose mapping to `5050:5050`.

## .NET 8 techniques used

- `System.IO.Pipelines` for efficient framed TCP reads
- `System.Threading.Channels` for producer/consumer handoff
- `Utf8JsonReader` for low-allocation JSON parsing
- `ConcurrentDictionary` plus `Interlocked` for lock-free counters
- Batched SQLite inserts inside one transaction instead of per-share writes

The goal is not a full production pool, but a compact, practical example of high-throughput share ingestion in modern .NET.
