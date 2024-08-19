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
}

