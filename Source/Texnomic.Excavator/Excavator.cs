namespace Texnomic.Excavator;

public abstract class Excavator<TTransformer, TLoader>(
    string ExcavatorName,
    IOptions<ExcavatorOptions> OptionsAccessor,
    ILogger Logger) : IHostedService
{
    private readonly Dictionary<string, Task> Threads = [];
    private readonly CancellationTokenSource CancellationTokenSource = new();
    protected readonly ExcavatorOptions Options = OptionsAccessor.Value;
    protected readonly Channel<TTransformer> TransformerQueue = Channel.CreateBounded<TTransformer>(OptionsAccessor.Value.TransformerQueueCapacity);
    protected readonly Channel<TLoader> LoaderQueue = Channel.CreateBounded<TLoader>(OptionsAccessor.Value.LoaderQueueCapacity);

    public virtual async Task StartAsync(CancellationToken CancellationToken)
    {
        try
        {
            Logger.Information("[{System}] Starting...", ExcavatorName);

            await Initializer(CancellationToken);

            Threads.Add(nameof(Extractor), Extractor(CancellationTokenSource.Token));
            Threads.Add(nameof(Transformer), Transformer(CancellationTokenSource.Token));
            Threads.Add(nameof(Loader), Loader(CancellationTokenSource.Token));
            Threads.Add(nameof(Reporter), Reporter(CancellationTokenSource.Token));

            Logger.Information("[{System}] Started.", ExcavatorName);
        }
        catch (OperationCanceledException) when (CancellationToken.IsCancellationRequested)
        {
            Logger.Warning("[{System}] [{Function}] Task Canceled.", ExcavatorName, nameof(StartAsync));
        }
        catch (TaskCanceledException) when (CancellationToken.IsCancellationRequested)
        {
            Logger.Warning("[{System}] [{Function}] Task Canceled.", ExcavatorName, nameof(StartAsync));
        }
        catch (Exception Error)
        {
            Logger.Fatal(Error, "[{System}] [{Function}] Function Error.", ExcavatorName, nameof(StartAsync));

            await StopAsync(CancellationToken);
        }
    }

    public virtual async Task StopAsync(CancellationToken CancellationToken)
    {
        try
        {
            Logger.Information("[{System}] Stopping...", ExcavatorName);

            await CancellationTokenSource.CancelAsync();

            TransformerQueue.Writer.Complete();

            LoaderQueue.Writer.Complete();

            await Task.WhenAll(Threads.Values);

            Logger.Information("[{System}] Stopped.", ExcavatorName);
        }
        catch (OperationCanceledException) when (CancellationToken.IsCancellationRequested)
        {
            Logger.Warning("[{System}] [{Function}] Task Canceled.", ExcavatorName, nameof(StopAsync));
        }
        catch (TaskCanceledException) when (CancellationToken.IsCancellationRequested)
        {
            Logger.Warning("[{System}] [{Function}] Task Canceled.", ExcavatorName, nameof(StopAsync));
        }
        catch (Exception Error)
        {
            Logger.Fatal(Error, "[{System}] [{Function}] Function Error.", ExcavatorName, nameof(StopAsync));
        }
    }

    protected virtual async Task Initializer(CancellationToken CancellationToken)
    {
        try
        {
            Logger.Information("[{System}] [{Function}] Started.", ExcavatorName, nameof(Initializer));

            await InitializerCore(CancellationToken);

            Logger.Information("[{System}] [{Function}] Stopped.", ExcavatorName, nameof(Initializer));
        }
        catch (OperationCanceledException) when (CancellationToken.IsCancellationRequested)
        {
            Logger.Warning("[{System}] [{Function}] Task Canceled.", ExcavatorName, nameof(Initializer));
        }
        catch (TaskCanceledException) when (CancellationToken.IsCancellationRequested)
        {
            Logger.Warning("[{System}] [{Function}] Task Canceled.", ExcavatorName, nameof(Initializer));
        }
        catch (Exception Error)
        {
            Logger.Fatal(Error, "[{System}] [{Function}] Function Error.", ExcavatorName, nameof(Initializer));

            await StopAsync(CancellationToken);
        }
    }

    protected abstract ValueTask InitializerCore(CancellationToken CancellationToken);

    protected virtual async Task Extractor(CancellationToken CancellationToken)
    {
        try
        {
            Logger.Information("[{System}] [{Function}] Started.", ExcavatorName, nameof(Extractor));

            while (CancellationToken.IsCancellationRequested is false)
            {
                await ExtractorCore(CancellationToken);
            }

            Logger.Information("[{System}] [{Function}] Stopped.", ExcavatorName, nameof(Extractor));
        }
        catch (OperationCanceledException) when (CancellationToken.IsCancellationRequested)
        {
            Logger.Warning("[{System}] [{Function}] Task Canceled.", ExcavatorName, nameof(Extractor));
        }
        catch (TaskCanceledException) when (CancellationToken.IsCancellationRequested)
        {
            Logger.Warning("[{System}] [{Function}] Task Canceled.", ExcavatorName, nameof(Extractor));
        }
        catch (Exception Error)
        {
            Logger.Fatal(Error, "[{System}] [{Function}] Function Error.", ExcavatorName, nameof(Extractor));

            await StopAsync(CancellationToken);
        }
    }

    protected abstract ValueTask ExtractorCore(CancellationToken CancellationToken);

    protected virtual async Task Transformer(CancellationToken CancellationToken)
    {
        try
        {
            Logger.Information("[{System}] [{Function}] Started.", ExcavatorName, nameof(Transformer));

            var ParallelOptions = new ParallelOptions()
            {
                CancellationToken = CancellationToken,
                MaxDegreeOfParallelism = Options.TransformerMaxDegreeOfParallelism
            };

            await Parallel.ForEachAsync(TransformerQueue.Reader.ReadAllAsync(CancellationToken), ParallelOptions, TransformerCore);

            Logger.Information("[{System}] [{Function}] Stopped.", ExcavatorName, nameof(Transformer));
        }
        catch (OperationCanceledException) when (CancellationToken.IsCancellationRequested)
        {
            Logger.Warning("[{System}] [{Function}] Task Canceled.", ExcavatorName, nameof(Transformer));
        }
        catch (TaskCanceledException) when (CancellationToken.IsCancellationRequested)
        {
            Logger.Warning("[{System}] [{Function}] Task Canceled.", ExcavatorName, nameof(Transformer));
        }
        catch (Exception Error)
        {
            Logger.Fatal(Error, "[{System}] [{Function}] Function Error.", ExcavatorName, nameof(Transformer));

            await StopAsync(CancellationToken);
        }
    }

    protected abstract ValueTask TransformerCore(TTransformer Context, CancellationToken CancellationToken);

    protected virtual async Task Loader(CancellationToken CancellationToken)
    {
        try
        {
            Logger.Information("[{System}] [{Function}] Started.", ExcavatorName, nameof(Loader));

            var ParallelOptions = new ParallelOptions()
            {
                CancellationToken = CancellationToken,
                MaxDegreeOfParallelism = Options.LoaderMaxDegreeOfParallelism
            };

            await Parallel.ForEachAsync(LoaderQueue.Reader.ReadAllAsync(CancellationToken), ParallelOptions, LoaderCore);
            
            Logger.Information("[{System}] [{Function}] Stopped.", ExcavatorName, nameof(Loader));
        }
        catch (OperationCanceledException) when (CancellationToken.IsCancellationRequested)
        {
            Logger.Warning("[{System}] [{Function}] Task Canceled.", ExcavatorName, nameof(Loader));
        }
        catch (TaskCanceledException) when (CancellationToken.IsCancellationRequested)
        {
            Logger.Warning("[{System}] [{Function}] Task Canceled.", ExcavatorName, nameof(Loader));
        }
        catch (Exception Error)
        {
            Logger.Fatal(Error, "[{System}] [{Function}] Function Error.", ExcavatorName, nameof(Loader));

            await StopAsync(CancellationToken);
        }
    }

    protected abstract ValueTask LoaderCore(TLoader Context, CancellationToken CancellationToken);

    protected virtual async Task Reporter(CancellationToken CancellationToken)
    {
        try
        {
            Logger.Information("[{System}] [{Function}] Started.", ExcavatorName, nameof(Reporter));

            var Delay = Options.ReporterInterval;

            await Task.Delay(Delay, CancellationToken);

            while (CancellationToken.IsCancellationRequested is false)
            {
                await ReporterCore();

                await Task.Delay(Delay, CancellationToken);
            }

            Logger.Information("[{System}] [{Function}] Stopped.", ExcavatorName, nameof(Reporter));
        }
        catch (OperationCanceledException) when (CancellationToken.IsCancellationRequested)
        {
            Logger.Warning("[{System}] [{Function}] Task Canceled.", ExcavatorName, nameof(Reporter));
        }
        catch (TaskCanceledException) when (CancellationToken.IsCancellationRequested)
        {
            Logger.Warning("[{System}] [{Function}] Task Canceled.", ExcavatorName, nameof(Reporter));
        }
        catch (Exception Error)
        {
            Logger.Fatal(Error, "[{System}] [{Function}] Function Error.", ExcavatorName, nameof(Reporter));

            await StopAsync(CancellationToken);
        }
    }

    protected virtual async ValueTask ReporterCore()
    {
        Logger.Information("[{System}] [{Name}] Transformer: {TransformerQueueCount} | Loader: {LoaderQueueCount}.",
            ExcavatorName,
            nameof(Reporter),
            $"{TransformerQueue.Reader.Count:N0}",
            $"{LoaderQueue.Reader.Count:N0}");
    }

    private async Task Worker<T>(ChannelReader<T> ChannelReader, Func<T, CancellationToken, ValueTask> Function, CancellationToken CancellationToken)
    {
        while (CancellationToken.IsCancellationRequested is false)
        {
            var Item = await ChannelReader.ReadAsync(CancellationToken);
            
            await Function(Item, CancellationToken);
        }
    }
}