namespace Texnomic.Excavator;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddExcavator<TExcavator>(this IServiceCollection Services)
        where TExcavator : class, IHostedService
    {
        Services.AddOptions<ExcavatorOptions>();
        Services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, TExcavator>());
        return Services;
    }

    public static IServiceCollection AddExcavator<TExcavator>(
        this IServiceCollection Services,
        Action<ExcavatorOptions> ConfigureOptions)
        where TExcavator : class, IHostedService
    {
        Services.Configure(ConfigureOptions);
        Services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, TExcavator>());
        return Services;
    }

    public static IServiceCollection AddExcavator<TExcavator, TExcavatorOptions>(this IServiceCollection Services)
        where TExcavator : class, IHostedService
        where TExcavatorOptions : ExcavatorOptions
    {
        Services.AddOptions<TExcavatorOptions>();
        Services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, TExcavator>());
        return Services;
    }

    public static IServiceCollection AddExcavator<TExcavator, TExcavatorOptions>(
        this IServiceCollection Services,
        Action<TExcavatorOptions> ConfigureOptions)
        where TExcavator : class, IHostedService
        where TExcavatorOptions : ExcavatorOptions
    {
        Services.Configure(ConfigureOptions);
        Services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, TExcavator>());
        return Services;
    }
}
