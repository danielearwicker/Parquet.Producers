namespace Parquet.Producers;

public enum PersistentStreamType
{
    KeyMappings,
    Content,
    Update
}

public interface IPersistentStreams
{
    // If the file does not exist, returns empty stream
    Task<Stream> OpenRead(string name, PersistentStreamType type, int version, string extension);

    // If the stream is empty, any existing file is deleted
    Task Upload(string name, PersistentStreamType type, int version, string extension, Stream content, CancellationToken cancellation);
}
