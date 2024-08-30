using MessagePack;
using Parquet.Producers.Serialization;

namespace Parquet.Producers.MessagePack;

public class MessagePackSerializationFormat(MessagePackSerializerOptions options) : ISerializationFormat
{
    public string Extension => "msgpack";

    public async ValueTask<ISerializationReader<T>> Read<T>(Stream stream) where T : new()
    {
        stream.Position = stream.Length - sizeof(long);
        stream.Position = new BinaryReader(stream).ReadInt64();

        var offsets = await MessagePackSerializer.DeserializeAsync<List<long>>(stream, options, default);

        stream.Position = offsets.Count < 2 ? 0 : offsets[^2];

        var metadata = await MessagePackSerializer.DeserializeAsync<Dictionary<string, string>>(stream, options, default);
        return new MessagePackSerializationReader<T>(stream, offsets, options, metadata);
    }
    
    public ValueTask<ISerializationWriter<T>> Write<T>(Stream stream) where T : new()
        => ValueTask.FromResult<ISerializationWriter<T>>(new MessagePackSerializationWriter<T>(stream, options));
}
