# Texnomic.Excavator

[![NuGet](https://img.shields.io/nuget/v/Texnomic.Excavator.svg)](https://www.nuget.org/packages/Texnomic.Excavator/)
[![Downloads](https://img.shields.io/nuget/dt/Texnomic.Excavator.svg)](https://www.nuget.org/packages/Texnomic.Excavator/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![.NET 10](https://img.shields.io/badge/.NET-10.0-512BD4.svg)](https://dotnet.microsoft.com/)

.NET 10 ETL host scaffolding for high-throughput data pipelines. `Excavator<TTransformer, TLoader>` wires Extract → Transform → Load stages over bounded `System.Threading.Channels`, runs the Transform and Load stages in parallel across all CPU cores, logs every stage transition through Serilog, and slots into any `IHostedService` host so lifecycle (start/stop/cancellation) is handled for you.

## Installation

```sh
dotnet add package Texnomic.Excavator
```

## Usage

Subclass `Excavator<TTransformer, TLoader>` and implement the four `*Core` hooks. The base class spins up four named worker loops — `Extractor`, `Transformer`, `Loader`, `Reporter` — each guarded by `OperationCanceledException` / `TaskCanceledException` handlers tied to the host's shutdown token.

```csharp
using Microsoft.Extensions.Options;
using Texnomic.Excavator;

public sealed class BlockExcavator(IOptions<ExcavatorOptions> Options, ILogger Logger)
    : Excavator<RawBlock, NormalizedBlock>("Blocks", Options, Logger)
{
    protected override ValueTask InitializerCore(CancellationToken CancellationToken)
    {
        // open db connections, prime caches, validate config
        return ValueTask.CompletedTask;
    }

    protected override async ValueTask ExtractorCore(CancellationToken CancellationToken)
    {
        var Raw = await Source.NextBlockAsync(CancellationToken);
        await TransformerQueue.Writer.WriteAsync(Raw, CancellationToken);
    }

    protected override async ValueTask TransformerCore(RawBlock Context, CancellationToken CancellationToken)
    {
        var Normalized = Normalize(Context);
        await LoaderQueue.Writer.WriteAsync(Normalized, CancellationToken);
    }

    protected override async ValueTask LoaderCore(NormalizedBlock Context, CancellationToken CancellationToken)
        => await Sink.InsertAsync(Context, CancellationToken);
}
```

Register it with the `AddExcavator<TExcavator>` extension method — a one-liner that wires both the hosted service and its `ExcavatorOptions`:

```csharp
var Builder = Host.CreateApplicationBuilder(args);

Builder.Services.AddSerilog((Services, Configuration) => Configuration
    .ReadFrom.Configuration(Builder.Configuration)
    .Enrich.FromLogContext());

// Defaults — equivalent to `new ExcavatorOptions()`
Builder.Services.AddExcavator<BlockExcavator>();

// ...or override the defaults with an Action<ExcavatorOptions>:
Builder.Services.AddExcavator<BlockExcavator>(Options =>
{
    Options.TransformerQueueCapacity         = 500;
    Options.LoaderQueueCapacity              = 500;
    Options.TransformerMaxDegreeOfParallelism = 16;
    Options.LoaderMaxDegreeOfParallelism      = 8;
    Options.ReporterInterval                  = TimeSpan.FromSeconds(30);
});

await Builder.Build().RunAsync();
```

When your excavator uses a derived options type, pass it as the second generic argument:

```csharp
Builder.Services.AddExcavator<BlockExcavator, BlockExcavatorOptions>(Options =>
{
    Options.Network                   = "mainnet";
    Options.BatchSize                 = 100;
    Options.TransformerQueueCapacity  = 500;   // inherited from ExcavatorOptions
});
```

To bind from configuration instead, call `Services.Configure<TExcavatorOptions>(Configuration.GetSection("..."))` first, then `AddExcavator<TExcavator>()` with no configure delegate.

### Options reference

| Property                            | Default                       | Effect                                                              |
|-------------------------------------|-------------------------------|---------------------------------------------------------------------|
| `TransformerQueueCapacity`          | `100`                         | Bound on the Extractor → Transformer channel.                       |
| `LoaderQueueCapacity`               | `100`                         | Bound on the Transformer → Loader channel.                          |
| `TransformerMaxDegreeOfParallelism` | `Environment.ProcessorCount`  | Parallel partitions reading from `TransformerQueue`.                |
| `LoaderMaxDegreeOfParallelism`      | `Environment.ProcessorCount`  | Parallel partitions reading from `LoaderQueue`.                     |
| `ReporterInterval`                  | `00:01:00`                    | Tick interval for the queue-depth Reporter log line.                |

### Extending ExcavatorOptions

`ExcavatorOptions` is an inheritable base. When your pipeline needs its own knobs — connection strings, batch sizes, feature flags — inherit it and close the open generic over your derived options type via `Excavator<TTransformer, TLoader, TExcavatorOptions>`:

```csharp
public sealed class BlockExcavatorOptions : ExcavatorOptions
{
    public string Network    { get; set; } = "mainnet";
    public int    BatchSize  { get; set; } = 100;
}

public sealed class BlockExcavator(IOptions<BlockExcavatorOptions> Options, ILogger Logger)
    : Excavator<RawBlock, NormalizedBlock, BlockExcavatorOptions>("Blocks", Options, Logger)
{
    protected override async ValueTask ExtractorCore(CancellationToken CancellationToken)
    {
        var Raw = await Source.NextBlockAsync(Options.Network, Options.BatchSize, CancellationToken);
        await TransformerQueue.Writer.WriteAsync(Raw, CancellationToken);
    }
}
```

`Options` is typed as your derived class, so you get both the inherited pipeline tuning (`TransformerQueueCapacity`, `MaxDegreeOfParallelism`, `ReporterInterval`) and your own properties on a single object. Register the derived options the same way:

```csharp
Builder.Services.Configure<BlockExcavatorOptions>(Builder.Configuration.GetSection("Blocks"));
```

The two-arg `Excavator<TTransformer, TLoader>` shown earlier is just a convenience that closes the third generic over plain `ExcavatorOptions` — pick whichever fits.

## How it works

```
            ┌───────────┐    bounded Channel<TTransformer>    ┌───────────────┐    bounded Channel<TLoader>    ┌────────┐
producers ─▶│ Extractor │ ──────────────────────────────────▶ │ Transformer × N │ ────────────────────────────▶ │ Loader │ ─▶ sink
            └───────────┘                                      └───────────────┘                                └────────┘
                  ▲                                                    ▲                                            ▲
                  └─────── Reporter (queue depths, every minute) ──────┴────────────────────────────────────────────┘
```

| Stage         | Concurrency                              | Backpressure                             |
|---------------|------------------------------------------|------------------------------------------|
| `Extractor`   | Single loop, awaits `ExtractorCore`      | Blocks on `TransformerQueue.Writer.WriteAsync` when downstream is full |
| `Transformer` | `Parallel.ForEachAsync`, `Environment.ProcessorCount` partitions | Blocks on `LoaderQueue.Writer.WriteAsync` |
| `Loader`      | `Parallel.ForEachAsync`, `Environment.ProcessorCount` partitions | Drains `LoaderQueue`                     |
| `Reporter`    | 1-minute tick, logs queue depths         | n/a                                      |

Both queues default to a capacity of 100 (`Channel.CreateBounded`). Override the constructor or expose them in your subclass to tune capacity, full-mode behavior, or single-reader/writer hints for your workload.

`StartAsync` brings all four loops up after `InitializerCore` returns. `StopAsync` cancels the internal `CancellationTokenSource`, completes both channel writers, and `await`s every worker `Task` so partial batches drain instead of being abandoned. Any exception inside a loop is logged at `Fatal` and triggers `StopAsync` — there is no zombie state.

## Building from source

```sh
git clone https://github.com/texnomic/Excavator
cd Excavator
dotnet build
```

## License

[MIT](LICENSE) © Texnomic.
