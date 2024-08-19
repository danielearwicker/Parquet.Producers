using Parquet.Producers.Serialization;
using Parquet.Serialization;

namespace Parquet.Producers.Parquet;

public class ParquetSerializationWriter<T>(ParquetWriter writer, Stream stream) : ISerializationWriter<T>
{
    public IReadOnlyDictionary<string, string> Metadata
    {
        set { writer.CustomMetadata = value; }
    } 

    public async ValueTask Add(IReadOnlyCollection<T> rows, CancellationToken cancellation)
        => await ParquetSerializer.SerializeRowGroupAsync(writer, rows, cancellation);
    
    public ValueTask Finish(CancellationToken cancellation)
    {
        writer.Dispose();

        stream.Position = 0;

        return ValueTask.CompletedTask;
    }
}
