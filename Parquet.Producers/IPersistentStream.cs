namespace Parquet.Producers;

public enum PersistentStreamType
{
    KeyMapping,
    Content
}

public interface IPersistentStream
{
    Stream OpenRead(PersistentStreamType type);

    Task Upload(PersistentStreamType type, Stream content);
}

