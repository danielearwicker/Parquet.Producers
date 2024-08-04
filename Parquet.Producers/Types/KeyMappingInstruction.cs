namespace Parquet.Producers.Types;

public class KeyMappingInstruction<SK, TK> : KeyMapping<SK, TK>, IDeletable
{
    public bool Deletion { get; set; }
}

