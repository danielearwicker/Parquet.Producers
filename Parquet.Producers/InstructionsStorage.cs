using Parquet.Producers.Types;
using Parquet.Producers.Util;
using Parquet.Producers.Parquet;

namespace Parquet.Producers;

public sealed class InstructionsStorage<SK, TK, TV> : IDisposable
{
    private readonly MergeSorter<ContentInstruction<TK, SK, TV>> _contentSorter;
    private readonly MergeSorter<KeyMappingInstruction<SK, TK>> _keyMappingSorter;
    
    public InstructionsStorage(
        ParquetProducerPlatformOptions platform, 
        ParquetProducerOptions<SK, TK, TV> options)
    {
        var contentComparers = Comparers.Build<ContentInstruction<TK, SK, TV>>();

        _contentSorter = new(
            options,
            platform with 
            { 
                LoggingPrefix = $"{platform.LoggingPrefix}.ContentInstructions"
            },
            contentComparers.By(
                contentComparers.By(x => x.TargetKey, options.TargetKeyComparer),
                contentComparers.By(x => x.SourceKey, options.SourceKeyComparer)));

        var mappingComparers = Comparers.Build<KeyMappingInstruction<SK, TK>>();

        _keyMappingSorter = new(
            options,
            platform with
            {
                LoggingPrefix = $"{platform.LoggingPrefix}.KeyMappingInstructions"
            },
            mappingComparers.By(
                mappingComparers.By(x => x.SourceKey, options.SourceKeyComparer),
                mappingComparers.By(x => x.TargetKey, options.TargetKeyComparer)));
    }

    public ValueTask Delete(SK? sourceKey, TK? targetKey)
        => AddInternal(sourceKey, targetKey, default, true);

    public ValueTask Add(SK? sourceKey, TK? targetKey, TV? targetValue)
        => AddInternal(sourceKey, targetKey, targetValue, false);

    private async ValueTask AddInternal(SK? sourceKey, TK? targetKey, TV? targetValue, bool deletion)
    {
        await _contentSorter.Add(new ContentInstruction<TK, SK, TV>
        {
            SourceKey = sourceKey,
            TargetKey = targetKey,
            Value = targetValue,
            Deletion = deletion
        });

        await _keyMappingSorter.Add(new KeyMappingInstruction<SK, TK>
        {
            SourceKey = sourceKey,
            TargetKey = targetKey,
            Deletion = deletion,
        });
    }

    public async ValueTask Finish()
    {
        await _contentSorter.Finish();
        await _keyMappingSorter.Finish();
    }

    public IAsyncEnumerable<ContentInstruction<TK, SK, TV>> ReadContentInstructions(CancellationToken cancellation) 
        => _contentSorter.Read(cancellation);

    public IAsyncEnumerable<KeyMappingInstruction<SK, TK>> ReadKeyMappingInstructions(CancellationToken cancellation)
        => _keyMappingSorter.Read(cancellation);

    public void Dispose()
    {
        _contentSorter.Dispose();
        _keyMappingSorter.Dispose();
    }
}

