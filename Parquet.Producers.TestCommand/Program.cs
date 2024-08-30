using Parquet.Producers.TestCommand;
using Microsoft.Extensions.Logging;
using Parquet.Producers;
using Parquet.Producers.Types;
using Parquet.Serialization.Attributes;
using Parquet.Producers.Util;
using Parquet.Serialization;
using Parquet.Producers.MessagePack;
using MessagePack;

using ILoggerFactory factory = LoggerFactory.Create(builder => builder.AddConsole());
ILogger logger = factory.CreateLogger("Program");

var storagePath = args[0];
var inputFile = args[1];
var basedOnVersion = int.Parse(args[2]);

var storage = new PersistentStreams(storagePath);

var platform = new ParquetProducerPlatformOptions
    {
        Logger = logger,
        VeryNoisyLogger = new ThrottledLogger(logger, TimeSpan.FromSeconds(1)),
    };

var format = new MessagePackSerializationFormat(MessagePackSerializerOptions.Standard.WithCompression(MessagePackCompression.Lz4BlockArray));

using var transactions = new Producer<string, ExampleTransaction, string, ExampleTransaction>(storage, "Transactions", ByUniqueId, platform, new()
{
    Format = format
});

var creditMatchingKeyBuilder = Comparers.Build<CreditMatchingKey?>();
var creditMatchingKeyComparer = creditMatchingKeyBuilder.By(
    creditMatchingKeyBuilder.By(x => x?.SupplierRef),
    creditMatchingKeyBuilder.By(x => x?.AbsAmount)
);

using var creditsAndInvoices = transactions.Produces("CreditsAndInvoices", ByAbsoluteAmountSupplierAndType, 
    new()
    {
        TargetKeyComparer = creditMatchingKeyComparer,
        Format = format,
    });

var sourceUpdates = ReadFile().Select(x => new SourceUpdate<string, ExampleTransaction> { Key = $"{basedOnVersion}", Value = x });
await transactions.UpdateAll(sourceUpdates, basedOnVersion, CancellationToken.None);

async IAsyncEnumerable<ExampleTransaction> ReadFile()
{
    using var stream = File.OpenRead(inputFile);

    await foreach (var record in ParquetSerializer.DeserializeAllAsync<ExampleTransaction>(stream))
    {
        yield return record;
    }

    // await Task.Delay(1);

    // for (var n = 0; n < 1_000_000; n++)
    // {
    //     yield return new ExampleTransaction
    //     {
    //         UniqueId = $"u-{n}",
    //         InvoiceNumber = $"i-{n}",
    //         InvoiceDate = new DateTime(2020, 1, 1).AddMilliseconds(n * 17),
    //         SupplierRef = $"s-{n}",
    //         SupplierName = $"sn-{n}",
    //         InvoiceAmount = n,
    //         PONumber = $"po-{n}",
    //         EnteredDate = new DateTime(2020, 1, 1).AddMilliseconds(n * 17),
    //         ORG = $"org-{n}",
    //         PayGroup = $"pg-{n}",
    //         SupplierSite = $"ss-{n}",
    //     };
    // }
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

