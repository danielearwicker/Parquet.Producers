namespace Parquet.Producers.Util;

public class TemporaryStream(string filePath)
    : FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite)
{
    public TemporaryStream() : this(Path.GetTempFileName()) { }

    public override void Close()
    {
        var name = Name;

        base.Close();

        System.IO.File.Delete(name);
    }
}

