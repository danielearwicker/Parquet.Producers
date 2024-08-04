using System.Diagnostics.CodeAnalysis;

namespace Parquet.Producers.Util;



public static class Comparers
{
    public record ComparerBuilder<T>()
    {
        public IComparer<T> By<K>(Func<T, K> keySelector, IComparer<K>? keyValueComparer = null)
        {
            keyValueComparer ??= Comparer<K>.Default;
            return Comparer<T>.Create((a, b) => keyValueComparer.Compare(keySelector(a), keySelector(b)));
        }

        public IComparer<T> By(params IComparer<T>[] all) => all.Aggregate((comparer1, comparer2) =>
            Comparer<T>.Create((a, b) =>
            {
                var compared = comparer1.Compare(a, b);
                if (compared != 0) return compared;

                return comparer2.Compare(a, b);
            }));
    }

    public static IComparer<T> Reverse<T>(this IComparer<T> comparer)
        => Comparer<T>.Create((a, b) => comparer.Compare(b, a));

    public static ComparerBuilder<T> Build<T>() => new();

    private sealed class EqualityComparerFromComparer<T>(IComparer<T> comparer) : IEqualityComparer<T>
    {
        public bool Equals(T? x, T? y) => comparer.Compare(x, y) == 0;

        public int GetHashCode([DisallowNull] T obj) => obj.GetHashCode();
    }

    public static IEqualityComparer<T> ToEqualityComparer<T>(this IComparer<T> comparer) => new EqualityComparerFromComparer<T>(comparer);
}

