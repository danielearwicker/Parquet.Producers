using Parquet.Serialization.Attributes;
using MessagePack;

[MessagePackObject]
public class CreditMatchingKey
{
    [Key(0)] [ParquetRequired] public string SupplierRef { get; set; } = string.Empty;
    [Key(1)] public decimal AbsAmount { get; set; }
    [Key(2)] public bool IsCredit { get; set; }
}

