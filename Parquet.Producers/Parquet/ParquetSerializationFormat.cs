using Parquet.Producers.Serialization;
using Parquet.Serialization;

namespace Parquet.Producers.Parquet;

public class ParquetSerializationFormat(ParquetOptions options) : ISerializationFormat
{
    public async ValueTask<ISerializationReader<T>> Read<T>(Stream stream) where T : new()
        => new ParquetSerializationReader<T>(await ParquetReader.CreateAsync(stream, options));
    
    public async ValueTask<ISerializationWriter<T>> Write<T>(Stream stream) where T : new()
        => new ParquetSerializationWriter<T>(await ParquetWriter.CreateAsync(
            typeof(T).GetParquetSchema(true), stream, options), stream);
}
