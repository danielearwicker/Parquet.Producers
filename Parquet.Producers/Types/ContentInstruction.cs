namespace Parquet.Producers.Types;

public class ContentInstruction<TK, SK, TV> : KeyMapping<SK, TK>, IDeletable
{
    public TV? Value { get; set; }

    public bool Deletion { get; set; }
}

