namespace Parquet.Producers.Types;

public class SourceUpdate<K, V>
{
    public bool Deletion { get; set; }

    public K? Key { get; set; }

    // Ignored if Deletion is true
    public V? Value { get; set; }
}
