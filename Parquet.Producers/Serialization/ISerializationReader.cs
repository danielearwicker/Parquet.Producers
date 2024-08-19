namespace Parquet.Producers.Serialization;

public interface ISerializationReader<T>
{
    ValueTask<IList<T>> Get(int rowGroup, CancellationToken cancellation);

    int RowGroupCount { get; }

    IReadOnlyDictionary<string, string> Metadata { get; }
}
