namespace Parquet.Producers.TestCommand;

using File = System.IO.File;

public class PersistentStreams(string rootDir) : IPersistentStreams
{
    private string GetPath(string name, PersistentStreamType type, int version, string extension)
        => Path.Combine(rootDir, $"{name}.{version}.{type}.{extension}");   

    public Task<Stream> OpenRead(string name, PersistentStreamType type, int version, string extension)
        => Task.FromResult<Stream>(File.Exists(GetPath(name, type, version, extension)) 
            ? File.OpenRead(GetPath(name, type, version, extension)) 
            : new MemoryStream());

    public Task Upload(string name, PersistentStreamType type, int version, string extension, Stream content, CancellationToken cancellation)
    {
        if (content.Length == 0)
        {
            File.Delete(GetPath(name, type, version, extension));
        }
        else
        {
            using var file = File.Create(GetPath(name, type, version, extension));
            content.CopyTo(file);
        }

        return Task.CompletedTask;
    }
}
