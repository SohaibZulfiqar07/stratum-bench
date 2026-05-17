using System.Buffers.Text;
using System.Diagnostics;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Serilog;

namespace MiniPoolMonitor.Core.Pipeline;

public sealed class ShareWorker : BackgroundService
{
    private readonly ChannelReader<ReadOnlyMemory<byte>> _shareReader;
    private readonly Serilog.ILogger _log;
    private long _totalSharesAccepted;

    public ShareWorker(ChannelReader<ReadOnlyMemory<byte>> shareReader)
    {
        _shareReader = shareReader ?? throw new ArgumentNullException(nameof(shareReader));
        _log = Log.ForContext<ShareWorker>();
    }

    public long TotalSharesAccepted => Interlocked.Read(ref _totalSharesAccepted);

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
        => RunAsync(stoppingToken).AsTask();

    private async ValueTask RunAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(500));
        var logTask = LogLoopAsync(timer, stoppingToken);

        try
        {
            while (await _shareReader.WaitToReadAsync(stoppingToken).ConfigureAwait(false))
            {
                while (_shareReader.TryRead(out ReadOnlyMemory<byte> message))
                {
                    if (TryParseMinerAndDifficulty(message.Span, out _, out _))
                        Interlocked.Increment(ref _totalSharesAccepted);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        finally
        {
            await logTask.ConfigureAwait(false);
        }
    }

    private async ValueTask LogLoopAsync(PeriodicTimer timer, CancellationToken stoppingToken)
    {
        long lastTotal = 0;
        long lastTimestamp = Stopwatch.GetTimestamp();

        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                long nowTotal = Interlocked.Read(ref _totalSharesAccepted);
                long nowTimestamp = Stopwatch.GetTimestamp();

                long delta = nowTotal - lastTotal;
                long elapsedTicks = nowTimestamp - lastTimestamp;

                double elapsedSeconds = elapsedTicks > 0
                    ? elapsedTicks / (double)Stopwatch.Frequency
                    : 0;

                double rate = elapsedSeconds > 0 ? delta / elapsedSeconds : 0;

                _log.Information("Shares/sec: {SharesPerSecond:F2} TotalAccepted: {TotalAccepted}", rate, nowTotal);

                lastTotal = nowTotal;
                lastTimestamp = nowTimestamp;
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }

    private static bool TryParseMinerAndDifficulty(
        ReadOnlySpan<byte> utf8Json,
        out ReadOnlySpan<byte> minerNameUtf8,
        out double difficulty)
    {
        minerNameUtf8 = default;
        difficulty = 0;

        bool hasMiner = false;
        bool hasDifficulty = false;

        var reader = new Utf8JsonReader(utf8Json, isFinalBlock: true, state: default);

        try
        {
            while (reader.Read())
            {
                if (reader.TokenType != JsonTokenType.PropertyName)
                    continue;

                if (IsMinerProperty(ref reader))
                {
                    if (!reader.Read())
                        break;

                    if (reader.TokenType == JsonTokenType.String)
                    {
                        if (!reader.HasValueSequence)
                        {
                            minerNameUtf8 = reader.ValueSpan;
                            hasMiner = minerNameUtf8.Length > 0;
                        }
                        else
                        {
                            // Input arrives as a contiguous ReadOnlyMemory<byte> in this app, so this should be rare.
                            hasMiner = false;
                        }
                    }

                    continue;
                }

                if (IsDifficultyProperty(ref reader))
                {
                    if (!reader.Read())
                        break;

                    if (reader.TokenType == JsonTokenType.Number)
                    {
                        hasDifficulty = reader.TryGetDouble(out difficulty);
                    }
                    else if (reader.TokenType == JsonTokenType.String)
                    {
                        if (!reader.HasValueSequence)
                        {
                            ReadOnlySpan<byte> span = reader.ValueSpan;
                            hasDifficulty = Utf8Parser.TryParse(span, out difficulty, out int consumed) && consumed == span.Length;
                        }
                        else
                        {
                            hasDifficulty = false;
                        }
                    }

                    continue;
                }
            }
        }
        catch (JsonException)
        {
            return false;
        }

        return hasMiner && hasDifficulty;
    }

    private static bool IsMinerProperty(ref Utf8JsonReader reader)
        => reader.ValueTextEquals("miner"u8) ||
           reader.ValueTextEquals("minerName"u8) ||
           reader.ValueTextEquals("worker"u8) ||
           reader.ValueTextEquals("workerName"u8) ||
           reader.ValueTextEquals("user"u8) ||
           reader.ValueTextEquals("username"u8) ||
           reader.ValueTextEquals("login"u8);

    private static bool IsDifficultyProperty(ref Utf8JsonReader reader)
        => reader.ValueTextEquals("difficulty"u8) ||
           reader.ValueTextEquals("diff"u8);
}
