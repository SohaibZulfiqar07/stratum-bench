using System.Collections.Concurrent;

namespace MiniPoolMonitor.Core.Stats;

public sealed class MinerStats
{
    private readonly ConcurrentDictionary<string, long> _sharesByMiner = new(StringComparer.Ordinal);

    public void AddShare(string miner)
    {
        if (string.IsNullOrWhiteSpace(miner))
            return;

        _sharesByMiner.AddOrUpdate(
            miner,
            addValueFactory: static _ => 1,
            updateValueFactory: static (_, current) => Interlocked.Increment(ref current));
    }

    public Dictionary<string, long> GetSnapshot()
        => new(_sharesByMiner);
}
