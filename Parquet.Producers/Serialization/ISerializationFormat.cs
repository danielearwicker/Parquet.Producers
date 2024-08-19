namespace Parquet.Producers.Serialization;

public interface ISerializationFormat
{
    ValueTask<ISerializationReader<T>> Read<T>(Stream stream) where T : new();

    ValueTask<ISerializationWriter<T>> Write<T>(Stream stream) where T : new();
}
