namespace Baksteen.Async;

using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

public sealed class ConcurrentProcessingQueue<T> : IAsyncDisposable
{
    private readonly Channel<T> _channel;
    private readonly Func<T, CancellationToken, ValueTask> _processItem;
    private readonly Task _pump;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private bool _disposedValue;

    public ConcurrentProcessingQueue(
        int queueCapacity,
        int maxDegreeOfParallelism,
        Func<T, CancellationToken, ValueTask> processItem,
        CancellationToken cancellationToken = default)
    {
        _channel = Channel.CreateBounded<T>(
            new BoundedChannelOptions(queueCapacity)
            {
                AllowSynchronousContinuations = true,
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false,   // I'm not sure if the ReadAllAsync() below counts as a single reader.. lets choose the safe option
                SingleWriter = false,
            }
        );

        _processItem = processItem;
        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        _pump = Task.Run(async () =>
        {
            try
            {
                await Parallel.ForEachAsync(
                    _channel.Reader.ReadAllAsync(),
                    new ParallelOptions
                    {
                        MaxDegreeOfParallelism = maxDegreeOfParallelism,
                        CancellationToken = _cancellationTokenSource.Token
                    },
                    _processItem
               ).ConfigureAwait(false);
            }
            catch(TaskCanceledException)
            {
                // this is expected when canceling, no need to propagate this
            }
        }, cancellationToken);
    }

    public ValueTask Enqueue(T item)
    {
        return _channel.Writer.WriteAsync(item, _cancellationTokenSource.Token);
    }

    public bool TryEnqueue(T item)
    {
        return _channel.Writer.TryWrite(item);
    }

    public async ValueTask WaitForCompletion()
    {
        _channel.Writer.TryComplete();
        await _pump.ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if(!_disposedValue)
        {
            _disposedValue = true;
            // see https://learn.microsoft.com/en-us/dotnet/standard/garbage-collection/implementing-disposeasync
            _cancellationTokenSource.Cancel();
            await WaitForCompletion().ConfigureAwait(false);
            _cancellationTokenSource.Dispose();
        }
    }
}
