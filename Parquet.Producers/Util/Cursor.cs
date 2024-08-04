namespace Parquet.Producers.Util;

public class Cursor<T>(IAsyncEnumerator<T> source, bool hasCurrent)
{        
    public bool Valid { get; private set; } = hasCurrent;

    public T Value => source.Current;

    public async ValueTask Next() => Valid = await source.MoveNextAsync();    
}

public static class Cursor
{
    public static async ValueTask<Cursor<T>> ToCursor<T>(this IAsyncEnumerable<T> source)
    {
        var e = source.GetAsyncEnumerator();
        return new Cursor<T>(e, await e.MoveNextAsync());
    }
}
