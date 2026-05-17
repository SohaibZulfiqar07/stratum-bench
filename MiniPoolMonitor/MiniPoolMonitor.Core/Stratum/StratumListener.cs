using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using System.IO.Pipelines;

namespace MiniPoolMonitor.Core.Stratum;

public sealed class StratumListener
{
    private readonly IPEndPoint _endpoint;
    private Socket? _listenSocket;

    public StratumListener(int port = 3333, Channel<ReadOnlyMemory<byte>>? shareChannel = null)
    {
        _endpoint = new IPEndPoint(IPAddress.Any, port);
        ShareChannel = shareChannel ?? Channel.CreateUnbounded<ReadOnlyMemory<byte>>(
            new UnboundedChannelOptions
            {
                SingleReader = false,
                SingleWriter = false,
                AllowSynchronousContinuations = false
            });
    }

    public Channel<ReadOnlyMemory<byte>> ShareChannel { get; }

    public ValueTask StartAsync(CancellationToken cancellationToken)
    {
        if (_listenSocket is not null)
            throw new InvalidOperationException("Listener already started.");

        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
        {
            NoDelay = true
        };

        socket.Bind(_endpoint);
        socket.Listen(backlog: 512);

        _listenSocket = socket;

        _ = AcceptLoopAsync(socket, cancellationToken);
        return ValueTask.CompletedTask;
    }

    public ValueTask StopAsync()
    {
        var socket = Interlocked.Exchange(ref _listenSocket, null);
        if (socket is null)
            return ValueTask.CompletedTask;

        try
        {
            socket.Close();
        }
        catch
        {
        }
        finally
        {
            socket.Dispose();
        }

        return ValueTask.CompletedTask;
    }

    private async ValueTask AcceptLoopAsync(Socket listenSocket, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            Socket? client = null;
            try
            {
                client = await listenSocket.AcceptAsync(cancellationToken).ConfigureAwait(false);
                client.NoDelay = true;

                _ = HandleConnectionAsync(client, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                client?.Dispose();
                break;
            }
            catch (ObjectDisposedException)
            {
                client?.Dispose();
                break;
            }
            catch
            {
                client?.Dispose();
                await Task.Delay(50, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async ValueTask HandleConnectionAsync(Socket clientSocket, CancellationToken cancellationToken)
    {
        await using var stream = new NetworkStream(clientSocket, ownsSocket: true);
        var reader = PipeReader.Create(stream);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                ReadResult result = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                ReadOnlySequence<byte> buffer = result.Buffer;

                while (TryReadLine(ref buffer, out ReadOnlySequence<byte> line))
                {
                    // Trim optional '\r' before '\n' without allocating strings.
                    if (!line.IsEmpty && line.Slice(line.Length - 1, 1).FirstSpan[0] == (byte)'\r')
                        line = line.Slice(0, line.Length - 1);

                    if (line.Length > 0)
                    {
                        // Copy the framed message into a contiguous buffer for downstream consumers.
                        // This allocates a byte[] per message, but avoids any string allocations in the hot read loop.
                        var payload = new byte[(int)line.Length];
                        line.CopyTo(payload);

                        await ShareChannel.Writer.WriteAsync(payload, cancellationToken).ConfigureAwait(false);
                    }
                }

                reader.AdvanceTo(buffer.Start, buffer.End);

                if (result.IsCompleted)
                    break;
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch
        {
        }
        finally
        {
            await reader.CompleteAsync().ConfigureAwait(false);
        }
    }

    private static bool TryReadLine(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> line)
    {
        SequencePosition? pos = buffer.PositionOf((byte)'\n');
        if (pos is null)
        {
            line = default;
            return false;
        }

        var newLinePos = pos.Value;
        line = buffer.Slice(0, newLinePos);
        buffer = buffer.Slice(buffer.GetPosition(1, newLinePos));
        return true;
    }
}
