using System.Text;
using System.Threading.Channels;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Hosting;
using Serilog;

namespace MiniPoolMonitor.Core.Persistence;

public sealed class ShareBatchWriter : BackgroundService
{
    public readonly record struct AcceptedShare(string Miner, double Difficulty, DateTimeOffset AcceptedAt);

    private static readonly TimeSpan FlushInterval = TimeSpan.FromSeconds(2);
    private const int MaxBatchSize = 2000;

    private readonly Channel<AcceptedShare> _channel;
    private readonly string _connectionString;
    private readonly Serilog.ILogger _log;

    public ShareBatchWriter(string sqlitePath = "minipoolmonitor.db")
    {
        if (string.IsNullOrWhiteSpace(sqlitePath))
            throw new ArgumentException("SQLite path must be non-empty.", nameof(sqlitePath));

        _connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = sqlitePath,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Shared,
            Pooling = true
        }.ToString();

        _channel = Channel.CreateUnbounded<AcceptedShare>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });

        _log = Log.ForContext<ShareBatchWriter>();
    }

    public ChannelWriter<AcceptedShare> Writer => _channel.Writer;

    public ValueTask EnqueueAsync(string miner, double difficulty, DateTimeOffset acceptedAt, CancellationToken cancellationToken = default)
        => _channel.Writer.WriteAsync(new AcceptedShare(miner, difficulty, acceptedAt), cancellationToken);

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
        => RunAsync(stoppingToken).AsTask();

    private async ValueTask RunAsync(CancellationToken stoppingToken)
    {
        await using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(stoppingToken).ConfigureAwait(false);
        await EnsureSchemaAsync(connection, stoppingToken).ConfigureAwait(false);

        using var timer = new PeriodicTimer(FlushInterval);
        var buffer = new List<AcceptedShare>(capacity: 256);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                // Drain whatever is available quickly.
                while (_channel.Reader.TryRead(out var share))
                {
                    buffer.Add(share);
                    if (buffer.Count >= MaxBatchSize)
                        await FlushAsync(connection, buffer, stoppingToken).ConfigureAwait(false);
                }

                // Wait either for new data or for the next flush tick.
                var tickTask = timer.WaitForNextTickAsync(stoppingToken);
                var dataTask = _channel.Reader.WaitToReadAsync(stoppingToken);

                if (await ValueTask.WhenAny(tickTask, dataTask).ConfigureAwait(false) == tickTask)
                {
                    await tickTask.ConfigureAwait(false);
                    if (buffer.Count > 0)
                        await FlushAsync(connection, buffer, stoppingToken).ConfigureAwait(false);
                }
                else
                {
                    await dataTask.ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        finally
        {
            // Best-effort final flush during shutdown.
            try
            {
                while (_channel.Reader.TryRead(out var share))
                    buffer.Add(share);

                if (buffer.Count > 0)
                    await FlushAsync(connection, buffer, CancellationToken.None).ConfigureAwait(false);
            }
            catch
            {
            }
        }
    }

    private static async ValueTask EnsureSchemaAsync(SqliteConnection connection, CancellationToken cancellationToken)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText =
            """
            CREATE TABLE IF NOT EXISTS shares(
              id INTEGER PRIMARY KEY,
              miner TEXT NOT NULL,
              difficulty REAL NOT NULL,
              accepted_at TEXT NOT NULL
            );
            """;

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask FlushAsync(SqliteConnection connection, List<AcceptedShare> buffer, CancellationToken cancellationToken)
    {
        if (buffer.Count == 0)
            return;

        // Swap buffer contents into a local batch so producers can keep enqueueing.
        var batch = new List<AcceptedShare>(buffer);
        buffer.Clear();

        try
        {
            await using var tx = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            await using var cmd = connection.CreateCommand();
            cmd.Transaction = tx;

            var sql = new StringBuilder(capacity: 64 + (batch.Count * 32));
            sql.Append("INSERT INTO shares(miner, difficulty, accepted_at) VALUES ");

            for (int i = 0; i < batch.Count; i++)
            {
                if (i != 0)
                    sql.Append(',');

                sql.Append("($m").Append(i).Append(", $d").Append(i).Append(", $t").Append(i).Append(')');

                var share = batch[i];
                cmd.Parameters.AddWithValue("$m" + i, share.Miner);
                cmd.Parameters.AddWithValue("$d" + i, share.Difficulty);
                cmd.Parameters.AddWithValue("$t" + i, share.AcceptedAt.UtcDateTime.ToString("O"));
            }

            cmd.CommandText = sql.ToString();
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _log.Error(ex, "SQLite batch flush failed (Count={Count})", batch.Count);

            // Put the batch back at the front so we don't drop accepted shares.
            // If the buffer has already accumulated, prepend by inserting at index 0.
            buffer.InsertRange(0, batch);
        }
    }
}
