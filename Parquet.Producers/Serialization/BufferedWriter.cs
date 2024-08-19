namespace Parquet.Producers.Serialization;

public class BufferedWriter<T>(
    ISerializationWriter<T> writer,
    int RowsPerGroup,
    CancellationToken cancellation)
{
    private readonly List<T> _buffer = [];

    public async ValueTask Add(T record)
    {
        _buffer.Add(record);
        if (_buffer.Count >= RowsPerGroup) await Flush();
    }

    private async ValueTask Flush()
    {
        await writer.Add(_buffer, cancellation);

        _buffer.Clear();
    }

    public async ValueTask Finish()
    {
        if (_buffer.Count != 0)
        {
            await Flush();
        }

        await writer.Finish(cancellation);
    }

    public async ValueTask AddRange(IAsyncEnumerable<T> source)
    {
        await foreach (var item in source)
        {
            await Add(item);
        }
    }
}
