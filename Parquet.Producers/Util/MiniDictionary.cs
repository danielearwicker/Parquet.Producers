namespace Parquet.Producers.Util;

internal class MiniDictionary<K, V>(IComparer<K> comparer)
{
    private readonly (K? Key, V? Value)[] _keys = new (K?, V?)[2];
    private int _count = 0;

    public void Store(K? key, V? value)
    {
        if (_count < 2)
        {
            _keys[_count++] = (key, value);
            return;
        }

        if (comparer.Compare(_keys[1].Key, key) == 0) return;

        _keys[0] = _keys[1];
        _keys[1] = (key, value);
    }

    public bool Matches(K? key, out V? value)
    {
        for (int i = 0; i < _count; i++)
        {
            if (comparer.Compare(_keys[i].Key, key) == 0)
            {
                value = _keys[i].Value;
                return true;
            }
        }

        value = default;
        return false;
    }
}
