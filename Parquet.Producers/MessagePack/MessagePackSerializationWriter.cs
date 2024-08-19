using Parquet.Producers.Serialization;
using MessagePack;

namespace Parquet.Producers.MessagePack;

public class MessagePackSerializationWriter<T>(
    Stream stream, MessagePackSerializerOptions options) : ISerializationWriter<T>
{
    private readonly List<long> _offsets = [];

    public IReadOnlyDictionary<string, string> Metadata { get; set; } = new Dictionary<string, string>();
    
    public async ValueTask Add(IReadOnlyCollection<T> rows, CancellationToken cancellation)
    {
        await MessagePackSerializer.SerializeAsync(stream, rows, options, cancellation);
        _offsets.Add(stream.Position);
    }
    
    public async ValueTask Finish(CancellationToken cancellation)
    {
        await MessagePackSerializer.SerializeAsync(stream, Metadata, options, cancellation);
        _offsets.Add(stream.Position);

        var offsetsPos = stream.Position;
        await MessagePackSerializer.SerializeAsync(stream, _offsets, options, cancellation);

        new BinaryWriter(stream).Write(offsetsPos);
        stream.Position = 0;
    }
}
