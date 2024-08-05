using Parquet.Producers.TestCommand;
using Microsoft.Extensions.Logging;
using Parquet.Producers;
using Parquet.Producers.Types;
using Parquet.Serialization.Attributes;
using Parquet.Producers.Util;

using ILoggerFactory factory = LoggerFactory.Create(builder => builder.AddConsole());
ILogger logger = factory.CreateLogger("Program");

var storagePath = args[0];
var inputFile = args[1];
var basedOnVersion = int.Parse(args[2]);

logger.LogInformation(
    "Using storage path: {StoragePath}, reading parquet file {InputFile}, basing on version {Version}", 
    storagePath, inputFile, basedOnVersion);

var storage = new PersistentStreams(storagePath);

var platform = new ParquetProducerPlatformOptions
    {
        Logger = logger,
        VeryNoisyLogger = new ThrottledLogger(logger, TimeSpan.FromSeconds(1)),
    };

using var transactions = new Producer<string, ExampleTransaction, string, ExampleTransaction>(storage, "Transactions", ByUniqueId, platform);

var creditMatchingKeyBuilder = Comparers.Build<CreditMatchingKey?>();
var creditMatchingKeyComparer = creditMatchingKeyBuilder.By(
    creditMatchingKeyBuilder.By(x => x?.SupplierRef),
    creditMatchingKeyBuilder.By(x => x?.AbsAmount)
);

using var creditsAndInvoices = transactions.Produces("CreditsAndInvoices", ByAbsoluteAmountSupplierAndType, 
    new ParquetProducerOptions<string, CreditMatchingKey, CreditMatchingValue>
    {
        TargetKeyComparer = creditMatchingKeyComparer
    });

var sourceUpdates = ReadFile().Select(x => new SourceUpdate<string, ExampleTransaction> { Key = inputFile, Value = x });
await transactions.Update(sourceUpdates, basedOnVersion, CancellationToken.None);

async IAsyncEnumerable<ExampleTransaction> ReadFile()
{
    using var stream = File.OpenRead(inputFile);
    await foreach (var record in platform.Read<ExampleTransaction>(stream, default))
    {
        yield return record;
    }
}

async IAsyncEnumerable<(string, ExampleTransaction)> ByUniqueId(string _, IAsyncEnumerable<ExampleTransaction> values)
{
    await foreach (var value in values)
    {
        yield return (value.UniqueId, value);
    }
}

async IAsyncEnumerable<(CreditMatchingKey, CreditMatchingValue)> ByAbsoluteAmountSupplierAndType(string uniqueId, IAsyncEnumerable<ExampleTransaction> values)
{
    await foreach (var value in values)
    {
        yield return (
            new CreditMatchingKey { SupplierRef = value.SupplierRef, AbsAmount = Math.Abs(value.InvoiceAmount), IsCredit = value.InvoiceAmount < 0 }, 
            new CreditMatchingValue { InvoiceNumber = value.InvoiceNumber, EnteredDate = value.EnteredDate });
    }
}

class CreditMatchingKey
{
    [ParquetRequired] public string SupplierRef { get; set; } = string.Empty;
    public decimal AbsAmount { get; set; }
    public bool IsCredit { get; set; }
}

class CreditMatchingValue
{
    [ParquetRequired] public string InvoiceNumber { get; set; } = string.Empty;
    public DateTime EnteredDate { get; set; }
}

class ExampleTransaction
{
    [ParquetRequired] public string UniqueId { get; set; } = string.Empty;
    [ParquetRequired] public string InvoiceNumber { get; set; } = string.Empty;
    public DateTime InvoiceDate { get; set; }
    [ParquetRequired] public string SupplierRef { get; set; } = string.Empty;
    [ParquetRequired] public string SupplierName { get; set; } = string.Empty;
    public decimal InvoiceAmount { get; set; }
    [ParquetRequired] public string PONumber { get; set; } = string.Empty;
    public DateTime EnteredDate { get; set; }
    [ParquetRequired] public string ORG { get; set; } = string.Empty;
    [ParquetRequired] public string PayGroup { get; set; } = string.Empty;
    [ParquetRequired] public string SupplierSite { get; set; } = string.Empty;
}

