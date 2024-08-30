using MessagePack;

namespace Parquet.Producers.Types;

public enum SourceUpdateType
{
    Add,
    Update,
    Delete
}

[MessagePackObject]
public class SourceUpdate<K, V>
{
    [Key(0)]
    public SourceUpdateType Type { get; set; }

    [Key(1)]
    public K? Key { get; set; }

    // Ignored if Type is Delete
    [Key(2)]
    public V? Value { get; set; }
}
