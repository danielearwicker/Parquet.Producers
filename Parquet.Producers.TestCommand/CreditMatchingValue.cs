using Parquet.Serialization.Attributes;
using MessagePack;

[MessagePackObject]
public class CreditMatchingValue
{
    [Key(0)] [ParquetRequired] public string InvoiceNumber { get; set; } = string.Empty;
    [Key(1)] public DateTime EnteredDate { get; set; }
}

