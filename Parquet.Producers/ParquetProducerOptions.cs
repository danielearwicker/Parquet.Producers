using Parquet.Producers.Parquet;
using Parquet.Producers.Serialization;

namespace Parquet.Producers;

public record ParquetProducerOptions
{
    public int RowsPerGroup { get; set; } = 100_000;

    public int GroupsPerBatch { get; set; } = 20;

    public ISerializationFormat Format { get; set; } = new ParquetSerializationFormat(new ParquetOptions());
}

public record ParquetProducerOptions<SK, TK, TV> : ParquetProducerOptions
{
    public IComparer<SK?> SourceKeyComparer { get; set; } = Comparer<SK?>.Default;

    public IComparer<TK?> TargetKeyComparer { get; set; } = Comparer<TK?>.Default;

    /// <summary>
    /// If provided, will be passed two values, the first being a newly generated
    /// value and the second being an example of a previous value with the same
    /// target key. This allows you to mutate properties of the new value to
    /// retain information from the old. The second value may be null in which
    /// case the target key has no previous example.
    /// 
    /// An example usage would be to populate a Guid property in the new value.
    /// If no example is available, a new Guid is generated. If an example is
    /// available, the existing Guid can be copied from it. This allows you to
    /// generate persistent Guids that are associated with the key.
    /// </summary>
    public Action<TV, TV?>? PreserveKeyValues { get; set; } = null;
}
