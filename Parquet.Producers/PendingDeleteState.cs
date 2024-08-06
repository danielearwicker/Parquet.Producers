using Parquet.Producers.Types;
using Parquet.Producers.Parquet;

namespace Parquet.Producers;

internal class PendingDeleteState<TK, TV>(
    BufferedWriter<SourceUpdate<TK, TV>>? updates,
    IComparer<TK> comparer)
{
    enum PendingDelete
    {
        None,
        Requested,
        RuledOut
    }

    private PendingDelete _state = PendingDelete.None;
    private TK? _target = default;

    private async ValueTask Flush()
    {
        await updates!.Add(new SourceUpdate<TK, TV>
        {
            Key = _target,
            Type = SourceUpdateType.Delete,
        });
    }

    public async ValueTask Finish()
    {
        if (_state == PendingDelete.Requested)
        {
            await Flush();
        }
    }

    public async ValueTask SendDelete(TK? key)
    {
        if (updates == null) return;
            
        switch (_state)
        {
            case PendingDelete.None:
                _target = key;
                _state = PendingDelete.Requested;
                break;

            case PendingDelete.Requested:
                if (comparer.Compare(key, _target) != 0)
                {
                    await Flush();
                    _target = key;
                }
                break;

            case PendingDelete.RuledOut:
                if (comparer.Compare(key, _target) != 0)
                {
                    _target = key;
                    _state = PendingDelete.Requested;                        
                }
                break;
        }
    }

    public async ValueTask SendUpsert(TK? key, TV? value, SourceUpdateType type)
    {
        if (updates == null) return;

        if (_state == PendingDelete.Requested)
        {
            if (comparer.Compare(key, _target) == 0)
            {
                _state = PendingDelete.RuledOut;
            }
            else
            {
                await Flush();
                _state = PendingDelete.None;
            }
        }
        else if (_state == PendingDelete.RuledOut)
        {
            if (comparer.Compare(key, _target) != 0)
            {
                _state = PendingDelete.None;
            }
        }

        await updates.Add(new SourceUpdate<TK, TV>
        {
            Key = key,
            Value = value,
            Type = type,
        });
    }
}
