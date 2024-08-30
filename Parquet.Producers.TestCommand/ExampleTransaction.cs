using Parquet.Serialization.Attributes;
using MessagePack;

[MessagePackObject]
public class ExampleTransaction
{
    [Key(0)] [ParquetRequired] public string UniqueId { get; set; } = string.Empty;
    [Key(1)] [ParquetRequired] public string InvoiceNumber { get; set; } = string.Empty;
    [Key(2)] public DateTime InvoiceDate { get; set; }
    [Key(3)] [ParquetRequired] public string SupplierRef { get; set; } = string.Empty;
    [Key(4)] [ParquetRequired] public string SupplierName { get; set; } = string.Empty;
    [Key(5)] public decimal InvoiceAmount { get; set; }
    [Key(6)] [ParquetRequired] public string PONumber { get; set; } = string.Empty;
    [Key(7)] public DateTime EnteredDate { get; set; }
    [Key(8)] [ParquetRequired] public string ORG { get; set; } = string.Empty;
    [Key(9)] [ParquetRequired] public string PayGroup { get; set; } = string.Empty;
    [Key(10)] [ParquetRequired] public string SupplierSite { get; set; } = string.Empty;
}

