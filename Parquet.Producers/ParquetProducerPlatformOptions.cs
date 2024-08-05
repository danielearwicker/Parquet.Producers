using Microsoft.Extensions.Logging;
using Parquet.Producers.Util;
using Parquet.Serialization;

namespace Parquet.Producers;

public record ParquetProducerPlatformOptions
{
    public ILogger? Logger { get; set; }

    public ILogger? VeryNoisyLogger { get; set; }

    public string LoggingPrefix { get; set; } = "ParquetProduction";

    public Func<string, Stream> CreateTemporaryStream = _ => new TemporaryStream();

    public ParquetOptions? ParquetOptions { get; set; }

    public IAsyncEnumerable<T> Read<T>(Stream stream, CancellationToken cancellation) where T : new()
        => stream.Length == 0 
            ? AsyncEnumerable.Empty<T>() 
            : ParquetSerializer.DeserializeAllAsync<T>(stream, ParquetOptions, cancellation);
}

