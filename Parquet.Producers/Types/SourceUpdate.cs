namespace Parquet.Producers.Types;

public enum SourceUpdateType
{
    Add,
    Update,
    Delete
}

public class SourceUpdate<K, V>
{
    public SourceUpdateType Type { get; set; }

    public K? Key { get; set; }

    // Ignored if Type is Delete
    public V? Value { get; set; }
}
