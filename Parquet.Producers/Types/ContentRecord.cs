using MessagePack;

namespace Parquet.Producers.Types;

[MessagePackObject]
public class ContentRecord<TK, SK, TV> : KeyMapping<SK, TK>
{
    [Key(2)]
    public TV? Value { get; set; }
}
