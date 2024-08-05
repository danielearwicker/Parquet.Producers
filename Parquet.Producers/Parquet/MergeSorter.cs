using SuperLinq.Async;
using Parquet.Serialization;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace Parquet.Producers.Parquet;

public sealed class MergeSorter<T>(
    ParquetProducerOptions options,
    ParquetProducerPlatformOptions platform,
    IComparer<T>? Comparer = null) : IDisposable
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

        try
        {
            await ParquetSerializer.SerializeAsync(_buffer, stream, new ParquetSerializerOptions
            {
                RowGroupSize = options.RowsPerGroup,
                ParquetOptions = platform.ParquetOptions,
            });

            _batches.Add(stream);
            stream = null;
        }
        finally
        {
            if (stream != null)
            {
                await stream.DisposeAsync();
            }
        }

        platform.Logger?.LogInformation("{LoggingPrefix}.MergerSorter: Saved sorted batch of {Count} records in {Elapsed}",
        platform.LoggingPrefix, _buffer.Count, timer.Elapsed);

        _buffer.Clear();
    }

    public IAsyncEnumerable<T> Read(CancellationToken cancellation)
    {
        foreach (var batch in _batches)
        {
            batch.Position = 0;
        }

        var batchReaders = _batches.Select(batch => platform.Read<T>(batch, cancellation)).ToList();

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

