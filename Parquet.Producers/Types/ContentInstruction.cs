using MessagePack;

namespace Parquet.Producers.Types;

[MessagePackObject]
public class ContentInstruction<TK, SK, TV> : KeyMapping<SK, TK>, IDeletable
{
    [Key(2)]
    public TV? Value { get; set; }

    [Key(3)]
    public bool Deletion { get; set; }
}

