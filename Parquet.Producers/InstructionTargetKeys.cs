using Parquet.Producers.Types;
using Parquet.Producers.Util;

namespace Parquet.Producers;

internal class InstructionTargetKeys<TK, SK, TV>(
    Cursor<ContentInstruction<TK, SK, TV>> instruction,
    IComparer<TK> comparer)
{
    private readonly TK?[] _keys = new TK?[2];
    private int _count = 0;

    public void Store()
    {
        if (!instruction.Valid) return;

        var tk = instruction.Value.TargetKey;

        if (_count < 2)
        {
            _keys[_count++] = tk;
            return;
        }

        if (comparer.Compare(_keys[1], tk) == 0) return;

        _keys[0] = _keys[1];
        _keys[1] = tk;
    }

    public bool Matches(TK? tk)
        => (_count >= 1 && comparer.Compare(_keys[0], tk) == 0) ||
            (_count >= 2 && comparer.Compare(_keys[1], tk) == 0);
}
