using FluentAssertions;
using Parquet.Producers.MessagePack;
using MessagePack;

namespace Parquet.Producers.Tests;

public sealed class MessagePackTests
{
    [MessagePackObject]
    public class Billy
    {
        [Key(0)]
        public string Egg { get; set; } = string.Empty;

        [Key(1)]
        public int Stuff { get; set; }
    }

    [Test]
    public async Task MessagePackRoundTrip()
    {
        var serialization = new MessagePackSerializationFormat(MessagePackSerializerOptions.Standard);

        var stream = new MemoryStream();

        var writer = await serialization.Write<Billy>(stream);

        await writer.Add(
            [
                new Billy { Egg = "Sunny side up", Stuff = 42 },
                new Billy { Egg = "Duck", Stuff = 66 }
            ],
            default);

        await writer.Finish(default);

        var reader = await serialization.Read<Billy>(stream);
        reader.RowGroupCount.Should().Be(1);

        var got = await reader.Get(0, default);
        got.Should().BeEquivalentTo(new Billy[]
        {
            new() { Egg = "Sunny side up", Stuff = 42 },
            new() { Egg = "Duck", Stuff = 66 }
        });
    }
}
