namespace Texnomic.Excavator;

public sealed class ExcavatorOptions
{
    public int TransformerQueueCapacity { get; set; } = 100;

    public int LoaderQueueCapacity { get; set; } = 100;

    public int TransformerMaxDegreeOfParallelism { get; set; } = Environment.ProcessorCount;

    public int LoaderMaxDegreeOfParallelism { get; set; } = Environment.ProcessorCount;

    public TimeSpan ReporterInterval { get; set; } = TimeSpan.FromMinutes(1);
}
