namespace Parquet.Producers.Serialization;

public interface ISerializationWriter<T>
{
    public ValueTask Add(IReadOnlyCollection<T> rows, CancellationToken cancellation);

    public ValueTask Finish(CancellationToken cancellation);

    IReadOnlyDictionary<string, string> Metadata { set; }
}
