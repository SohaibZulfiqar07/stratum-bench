using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MiniPoolMonitor.Core.Pipeline;
using MiniPoolMonitor.Core.Persistence;
using MiniPoolMonitor.Core.Stats;
using MiniPoolMonitor.Core.Stratum;
using Serilog;
using System.Diagnostics;
using System.Threading.Channels;

Log.Logger = new LoggerConfiguration()
    .WriteTo.Console()
    .CreateLogger();

try
{
    long startedAt = Stopwatch.GetTimestamp();

    var builder = WebApplication.CreateBuilder(args);
    builder.Host.UseSerilog();
    builder.WebHost.UseUrls("http://0.0.0.0:5050");

    builder.Services.AddSingleton<MinerStats>();

    builder.Services.AddSingleton<StratumListener>();
    builder.Services.AddHostedService<StratumListenerHostedService>();

    builder.Services.AddSingleton<ShareBatchWriter>();
    builder.Services.AddHostedService(sp => sp.GetRequiredService<ShareBatchWriter>());

    builder.Services.AddSingleton<ShareWorker>(sp =>
    {
        var listener = sp.GetRequiredService<StratumListener>();
        ChannelReader<ReadOnlyMemory<byte>> reader = listener.ShareChannel.Reader;
        return new ShareWorker(reader);
    });
    builder.Services.AddHostedService(sp => sp.GetRequiredService<ShareWorker>());

    var app = builder.Build();

    app.MapGet("/stats", (ShareWorker worker, MinerStats minerStats) =>
    {
        long now = Stopwatch.GetTimestamp();
        long uptimeSeconds = (long)((now - startedAt) / (double)Stopwatch.Frequency);

        return new
        {
            totalSharesAccepted = worker.TotalSharesAccepted,
            perMinerShareCounts = minerStats.GetSnapshot(),
            uptimeSeconds
        };
    });

    await app.RunAsync().ConfigureAwait(false);
}
catch (Exception ex)
{
    Log.Fatal(ex, "MiniPoolMonitor terminated unexpectedly");
}
finally
{
    Log.CloseAndFlush();
}

internal sealed class StratumListenerHostedService : IHostedService
{
    private readonly StratumListener _listener;
    private CancellationTokenSource? _cts;

    public StratumListenerHostedService(StratumListener listener)
        => _listener = listener;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _cts = new CancellationTokenSource();
        return _listener.StartAsync(_cts.Token).AsTask();
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        var cts = Interlocked.Exchange(ref _cts, null);
        if (cts is not null)
        {
            try { cts.Cancel(); } catch { }
            cts.Dispose();
        }

        return _listener.StopAsync().AsTask();
    }
}
