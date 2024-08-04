namespace Parquet.Producers.Types;

public class ContentRecord<TK, SK, TV> : KeyMapping<SK, TK>
{
    public TV? Value;
}
