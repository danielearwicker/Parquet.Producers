using SuperLinq.Async;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace Parquet.Producers.Serialization;

public sealed class MergeSorter<T>(
    
    ParquetProducerOptions options,
    ParquetProducerPlatformOptions platform,
    IComparer<T>? Comparer,
    CancellationToken cancellation) : IDisposable
    where T : new()
{
    private readonly List<T> _buffer = [];
    private readonly List<Stream> _batches = [];

    private readonly int _maxBatchSize = options.RowsPerGroup * options.GroupsPerBatch;

    public async ValueTask Add(T record)
    {
        _buffer.Add(record);
        if (_buffer.Count >= _maxBatchSize) await Finish();
    }

    public async ValueTask Finish()
    {
        if (_buffer.Count == 0) return;

        platform.Logger?.LogInformation("{LoggingPrefix}.MergerSorter: Sorting {Count} records",
            platform.LoggingPrefix, _buffer.Count);

        var timer = new Stopwatch();
        timer.Start();

        _buffer.Sort(Comparer);

        timer.Stop();

        platform.Logger?.LogInformation("{LoggingPrefix}.MergerSorter: Sorted {Count} records in {Elapsed}",
            platform.LoggingPrefix, _buffer.Count, timer.Elapsed);

        timer.Restart();

        var stream = platform.CreateTemporaryStream($"{platform.LoggingPrefix}.MergerSorter[{_batches.Count}]");

        var writer = await options.Format.Write<T>(stream);

        try
        {
            for (var i = 0; i < _buffer.Count; i += options.RowsPerGroup)
            {
                await writer.Add(_buffer.GetRange(i, Math.Min(options.RowsPerGroup, _buffer.Count - i)), cancellation);
            }
            
            await writer.Finish(cancellation);

            _batches.Add(stream);
            stream = null;
        }
        finally
        {
            if (stream != null) await stream.DisposeAsync();
        }

        platform.Logger?.LogInformation("{LoggingPrefix}.MergerSorter: Saved sorted batch of {Count} records in {Elapsed}",
        platform.LoggingPrefix, _buffer.Count, timer.Elapsed);

        _buffer.Clear();
    }

    private async IAsyncEnumerable<T> ReadBatch(Stream stream)
    {
        var reader = await options.Format.Read<T>(stream);

        for (var i = 0; i < reader.RowGroupCount; i++)
        {
            var group = await reader.Get(i, cancellation);
            for (var r = 0; r < group.Count; r++)
            {
                yield return group[r];
            }
        }
    }

    public IAsyncEnumerable<T> Read()
    {
        foreach (var batch in _batches)
        {
            batch.Position = 0;
        }

        var batchReaders = _batches.Select(ReadBatch).ToList();

        if (batchReaders.Count == 0) return AsyncEnumerable.Empty<T>();
        if (batchReaders.Count == 1) return batchReaders[0];

        return batchReaders[0].SortedMerge(Comparer, batchReaders.Skip(1).ToArray());
    }

    public void Dispose()
    {
        foreach (var batch in _batches)
        {
            batch.Dispose();
        }
    }
}

