using MessagePack;
using Parquet.Producers.Serialization;

namespace Parquet.Producers.MessagePack;

public class MessagePackSerializationReader<T>(
    Stream stream,
    List<long> offsets,
    MessagePackSerializerOptions options,
    IReadOnlyDictionary<string, string> metadata) : ISerializationReader<T> where T : new()
{
    public int RowGroupCount => offsets.Count - 1;

    public IReadOnlyDictionary<string, string> Metadata => metadata;

    public async ValueTask<IList<T>> Get(int rowGroup, CancellationToken cancellation)
    {
        var end = offsets[rowGroup + 1];
        var start = rowGroup == 0 ? 0 : offsets[rowGroup - 1];
        var size = (int)(end - start);
        var buffer = new byte[size];

        stream.Position = start;
#if NET8_0_OR_GREATER
        await stream.ReadExactlyAsync(buffer, 0, size, cancellation);
#else
        var at = 0;
        while (size > 0)
        {
            var got = await stream.ReadAsync(buffer, at, size, cancellation);
            at += got;
            size -= got;
        }
#endif
        return await MessagePackSerializer.DeserializeAsync<IList<T>>(
            new MemoryStream(buffer), options, cancellation);
    }
}
