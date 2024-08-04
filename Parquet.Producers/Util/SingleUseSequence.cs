namespace Parquet.Producers.Util;

public sealed class SingleUseSequence<TSource, TTarget>(
    IAsyncEnumerator<TSource> source,
    Func<TSource, TTarget> select,
    Func<TSource, bool> terminator) : IAsyncEnumerable<TTarget>, IAsyncEnumerator<TTarget>
{
    private bool _used;

    public bool HasCurrent { get; private set; } = true;

    public TTarget Current { get; set; } = default!;

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public async ValueTask<bool> MoveNextAsync()
    {
        if (!HasCurrent) return false;
        if (terminator(source.Current)) return false;

        Current = select(source.Current);
        HasCurrent = await source.MoveNextAsync();
        return true;
    }

    public IAsyncEnumerator<TTarget> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        if (_used) throw new InvalidOperationException("SingleUseSequence can only be used once");

        _used = true;
        return this;
    }
}

