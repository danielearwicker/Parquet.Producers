using MessagePack;

namespace Parquet.Producers.Types;

[MessagePackObject]
public class KeyMapping<SK, TK>
{
    [Key(0)]
    public SK? SourceKey { get; set; }

    [Key(1)]
    public TK? TargetKey { get; set; }
}
