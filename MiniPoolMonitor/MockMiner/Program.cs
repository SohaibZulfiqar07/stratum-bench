using System.Net.Sockets;
using System.Text;

var names = new[]
{
    "alpha",
    "bravo",
    "charlie",
    "delta",
    "echo",
};

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

var rng = Random.Shared;
int id = 1;

using var client = new TcpClient();
await client.ConnectAsync("127.0.0.1", 3333, cts.Token);

await using var stream = client.GetStream();
await using var writer = new StreamWriter(stream, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false))
{
    AutoFlush = true,
    NewLine = "\n"
};

using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(100)); // 10 msgs/sec

while (await timer.WaitForNextTickAsync(cts.Token))
{
    string miner = names[rng.Next(names.Length)];
    double difficulty = rng.Next(1, 129);

    // Newline-delimited JSON-RPC-ish payload.
    string json =
        $$"""{"id":{{id++}},"jsonrpc":"2.0","method":"mining.submit","params":{"miner":"{{miner}}","difficulty":{{difficulty}}}}""";

    await writer.WriteLineAsync(json.AsMemory(), cts.Token);
}
