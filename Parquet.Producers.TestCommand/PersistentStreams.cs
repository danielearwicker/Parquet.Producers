namespace Parquet.Producers.TestCommand;

using File = System.IO.File;

public class PersistentStreams(string rootDir) : IPersistentStreams
{
    private string GetPath(string name, PersistentStreamType type, int version)
        => Path.Combine(rootDir, $"{name}.{version}.{type}.parquet");

    public Task<Stream> OpenRead(string name, PersistentStreamType type, int version)
        => Task.FromResult<Stream>(File.Exists(GetPath(name, type, version)) 
            ? File.OpenRead(GetPath(name, type, version)) 
            : new MemoryStream());

    public Task Upload(string name, PersistentStreamType type, int version, Stream content, CancellationToken cancellation)
    {
        if (content.Length == 0)
        {
            File.Delete(GetPath(name, type, version));
        }
        else
        {
            using var file = File.Create(GetPath(name, type, version));
            content.CopyTo(file);
        }

        return Task.CompletedTask;
    }
}
