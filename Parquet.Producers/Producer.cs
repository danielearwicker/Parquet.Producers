using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Parquet.Producers.Types;

namespace Parquet.Producers.TestCommand;

public interface IProducer : IDisposable
{
    string Name { get; }

    Stream Content { get; }

    Stream Updates { get; }

    IEnumerable<IProducer> Sources { get; }

    IEnumerable<IProducer> Targets { get; }

    void AddTarget(IProducer consumer);

    Task UpdateFromSources(int basedOnVersion, CancellationToken cancellation);
}

public interface IProducer<K, V> : IProducer
{
    IAsyncEnumerable<SourceUpdate<K, V>> ReadUpdates(CancellationToken cancellation);
}

public sealed class Producer<SK, SV, TK, TV> : IProducer<TK, TV>
{
    private readonly ParquetProduction<SK, SV, TK, TV> _production;
    private readonly Produce<SK, SV, TK, TV> _produce;
    private readonly ParquetProducerPlatformOptions _basePlatform;
    private readonly ParquetProducerPlatformOptions _platform;
    
    private readonly IPersistentStreams _storage;
    private readonly IReadOnlyList<IProducer<SK, SV>> _sources;
    private readonly HashSet<IProducer> _targets = [];

    public Producer(
        IPersistentStreams storage,
        string name,
        Produce<SK, SV, TK, TV> produce,
        ParquetProducerPlatformOptions? platform = null,
        ParquetProducerOptions<SK, TK, TV>? options = null,
        params IProducer<SK, SV>[] sources)
    {
        _basePlatform = platform ?? new ParquetProducerPlatformOptions();
        _platform = _basePlatform with { LoggingPrefix = $"{_basePlatform.LoggingPrefix}.{name}" };
        _production = new(_platform, options ?? new ParquetProducerOptions<SK, TK, TV>());

        _storage = storage;
        _produce = produce;
        _sources = sources;

        Name = name;

        KeyMappings = _platform.CreateTemporaryStream(nameof(KeyMappings));
        Content = _platform.CreateTemporaryStream(nameof(Content));
        Updates = _platform.CreateTemporaryStream(nameof(Updates));

        foreach (var source in sources)
        {
            source.AddTarget(this);
        }
    }

    public IProducer<TK2, TV2> Produces<TK2, TV2>(
        string name,
        Produce<TK, TV, TK2, TV2> produce,
        ParquetProducerOptions<TK, TK2, TV2>? options = null,
        params IProducer<TK, TV>[] additionalSources)
            => new Producer<TK, TV, TK2, TV2>(
                _storage, name, produce, _basePlatform, options, 
                [.. additionalSources, this]);

    public void AddTarget(IProducer consumer)
    {
        if (!_targets.Add(consumer))
        {
            throw new InvalidOperationException($"Consumer '{consumer.Name}' already added to '{Name}'");
        }
    }

    public IEnumerable<IProducer> Sources => _sources;
    public IEnumerable<IProducer> Targets => _targets;

    public string Name { get; }

    public Stream KeyMappings { get; }
    public Stream Content { get; }
    public Stream Updates { get; }

    public void Dispose()
    {
        KeyMappings.Dispose();
        Content.Dispose();
        Updates.Dispose();
    }

    public IAsyncEnumerable<SourceUpdate<TK, TV>> ReadUpdates(CancellationToken cancellation)
        => _platform.Read<SourceUpdate<TK, TV>>(Updates, cancellation);
    
    private async Task UpdateInternal(IAsyncEnumerable<SourceUpdate<SK, SV>> sourceUpdates, int basedOnVersion, CancellationToken cancellation)
    {
        using var PreviousMappings = await _storage.OpenRead(Name, PersistentStreamType.KeyMappings, basedOnVersion);
        using var PreviousContent = await _storage.OpenRead(Name, PersistentStreamType.Content, basedOnVersion);

        await _production.Update(
            PreviousMappings, KeyMappings,
            PreviousContent, Content,
            sourceUpdates, _produce,
            Updates, cancellation);

        var newVersion = basedOnVersion + 1;
        await _storage.Upload(Name, PersistentStreamType.KeyMappings, newVersion, KeyMappings, cancellation);
        await _storage.Upload(Name, PersistentStreamType.Content, newVersion, Content, cancellation);
        await _storage.Upload(Name, PersistentStreamType.Update, newVersion, Updates, cancellation);
    }

    private static void CollectTargets(HashSet<IProducer> transitiveTargets, IProducer producer)
    {
        foreach (var target in producer.Targets)
        {
            if (!transitiveTargets.Add(target)) continue;
            CollectTargets(transitiveTargets, target);
        }
    }

    private void AddToSequence(List<IProducer> sequence, IProducer producer)
    {
        if (producer == this || sequence.Contains(producer)) return;
        
        // Put its sources before it
        foreach (var source in producer.Sources)
        {
            AddToSequence(sequence, source);
        }

        sequence.Add(producer);
    }

    public async Task Update(IAsyncEnumerable<SourceUpdate<SK, SV>> sourceUpdates, int basedOnVersion, CancellationToken cancellation)
    {
        var transitiveTargets = new HashSet<IProducer>();
        CollectTargets(transitiveTargets, this);

        var sequence = new List<IProducer>();
        foreach (var target in transitiveTargets)
        {
            AddToSequence(sequence, target);
        }

        var timings = new List<(string, TimeSpan)>();
        var timer = new Stopwatch();

        timer.Start();
        await UpdateInternal(sourceUpdates, basedOnVersion, cancellation);
        timings.Add((Name, timer.Elapsed));

        foreach (var producer in sequence)
        {
            timer.Restart();
            await producer.UpdateFromSources(basedOnVersion, cancellation);
            timings.Add((producer.Name, timer.Elapsed));
        }

        foreach (var (producer, elapsed) in timings)
        {
            _platform.Logger?.LogInformation("{LoggingPrefix}.{Producer} updated in {Elapsed}",
                _platform.LoggingPrefix, producer, elapsed);
        }

        _platform.Logger?.LogInformation("{LoggingPrefix} Total time {Elapsed}",
                _platform.LoggingPrefix, TimeSpan.FromSeconds(timings.Sum(x => x.Item2.TotalSeconds)));
    }

    public Task UpdateFromSources(int basedOnVersion, CancellationToken cancellation)
    {
        var sourceUpdates = _sources.Count == 1
            ? _sources[0].ReadUpdates(cancellation)
            : _production.ReadSources(Sources.Select(x => (x.Updates, x.Content)).ToArray(), cancellation);

        return UpdateInternal(sourceUpdates, basedOnVersion, cancellation);
    }
}
