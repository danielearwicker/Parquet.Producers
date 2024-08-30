using MessagePack;

namespace Parquet.Producers.Types;

[MessagePackObject]
public class KeyMappingInstruction<SK, TK> : KeyMapping<SK, TK>, IDeletable
{
    [Key(2)]
    public bool Deletion { get; set; }
}

