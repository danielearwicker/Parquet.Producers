using Parquet.Producers.Serialization;
using Parquet.Serialization;

namespace Parquet.Producers.Parquet;

public class ParquetSerializationReader<T>(ParquetReader reader) : ISerializationReader<T> where T : new()
{
    public int RowGroupCount => reader.RowGroupCount;

    public IReadOnlyDictionary<string, string> Metadata => reader.CustomMetadata;

    public async ValueTask<IList<T>> Get(int rowGroup, CancellationToken cancellation)
        => await ParquetSerializer.DeserializeAsync<T>(reader.OpenRowGroupReader(rowGroup), reader.Schema, cancellation);    
}
