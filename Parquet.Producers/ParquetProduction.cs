using SuperLinq.Async;
using Parquet.Producers.Types;
using Parquet.Producers.Util;
using System.Runtime.CompilerServices;
using Parquet.Producers.Parquet;

namespace Parquet.Producers;

public delegate IAsyncEnumerable<(TK Key, TV Value)> 
    Produce<SK, SV, TK, TV>(SK key, IAsyncEnumerable<SV> values);

public class ParquetProduction<SK, SV, TK, TV>(ParquetProductionOptions<SK, TK>? options = null)
{
    private ParquetProductionOptions<SK, TK> _options = options ?? new ParquetProductionOptions<SK, TK>();

    /// <summary>
    /// Updates a sorted dataset. It is represented by two Parquet tables:
    /// 
    /// - KeyMapping: order by (SourceKey, TargetKey) and does not include a value
    /// - ContentRecord: order by (TargetKey) and includes a value
    /// 
    /// Separate streams are used for reading the previous and writing the updated
    /// versions of both these datasets, for flexibility in versioning.
    /// 
    /// The content is defined by a production function <c>Produce</c>
    /// that consumes a source dataset. It is called for each key in the source
    /// and returns a sequence of projected key-value pairs. These results can be
    /// yielded in any order.
    /// 
    /// The <c>sourceUpdates</c> parameter describes the changes to be made. It
    /// must by ordered by <c>Key</c>. There are two kinds of <c>SourceUpdate</c>:
    /// 
    /// - Deletion: only specifies a <c>Key</c> (<c>Value</c> is ignored). Causes 
    /// all records previously projected from that key to be deleted.
    /// - Upsert: has Deletion = false. Causes all records previously projected 
    /// from that key to be replaced with newly projected records.
    /// 
    /// </summary>
    /// <param name="previousKeyMappings"></param>
    /// <param name="updatedKeyMappings"></param>
    /// <param name="previousContent"></param>
    /// <param name="updatedContent"></param>
    /// <param name="sourceUpdates"></param>
    /// <param name="production"></param>
    /// <param name="targetUpdates"></param>
    /// <param name="cancellation"></param>
    /// <returns></returns>
    public async Task Update(
        Stream previousKeyMappings,
        Stream updatedKeyMappings,
        Stream previousContent,
        Stream updatedContent,
        IAsyncEnumerable<SourceUpdate<SK, SV>> sourceUpdates,
        Produce<SK, SV, TK, TV> production,
        Stream? targetUpdates = null,
        CancellationToken cancellation = default)
    {
        using var instructions = new InstructionsStorage<SK, TK, TV>(_options);

        var keyMappingsStartPosition = previousKeyMappings.Position;

        var keyMappings = _options.Read<KeyMapping<SK, TK>>(previousKeyMappings, cancellation);

        await GenerateInstructions(keyMappings, sourceUpdates, instructions, production, cancellation);

        previousKeyMappings.Position = keyMappingsStartPosition;

        await UpdateStream<KeyMapping<SK, TK>, KeyMappingInstruction<SK, TK>>(
            _options.RowsPerGroup, previousKeyMappings, updatedKeyMappings,
            instructions.ReadKeyMappingInstructions(cancellation), 
            ExecuteInstructionsOnMappings, cancellation);

        var updatesWriter = targetUpdates != null
            ? new BufferedWriter<SourceUpdate<TK, TV>>(targetUpdates, _options.RowsPerGroup, _options.ParquetOptions)
            : null;

        await UpdateStream<ContentRecord<TK, SK, TV>, ContentInstruction<TK, SK, TV>>(
            _options.RowsPerGroup, previousContent, updatedContent,
            instructions.ReadContentInstructions(cancellation), 
            (inst, prev) => ExecuteInstructionsOnContent(inst, prev, updatesWriter), 
            cancellation);

        if (updatesWriter != null)
        {
            await updatesWriter.Finish();
        }
    }

    public class KeyOnly
    {
        public SK? Key { get; set; }
    }

    public class ContentRecordFromSource
    {
        public SK? TargetKey { get; set; } // Because the target of the production is the source of the next production!

        public SV? Value;
    }

    private async IAsyncEnumerable<SourceUpdate<SK, SV>> ReadKeys(
        Stream updatesStream, 
        Stream contentStream, 
        IAsyncEnumerable<SK> keys,
        [EnumeratorCancellation] CancellationToken cancellation)
    {
        updatesStream.Position = 0;
        contentStream.Position = 0;

        var updates = _options.Read<SourceUpdate<SK, SV>>(updatesStream, cancellation);
        var content = _options.Read<ContentRecordFromSource>(contentStream, cancellation);

        await using var updatesEnumerator = updates.GetAsyncEnumerator(cancellation);
        var haveUpdate = await updatesEnumerator.MoveNextAsync();

        await using var contentEnumerator = content.GetAsyncEnumerator(cancellation);
        var haveContent = await contentEnumerator.MoveNextAsync();

        await foreach (var key in keys)
        {
            var suppressCurrent = false;

            while (haveUpdate)
            {
                var compared = _options.SourceKeyComparer.Compare(updatesEnumerator.Current.Key, key);

                if (compared == 0)
                {
                    yield return updatesEnumerator.Current;
                    suppressCurrent = true;
                }
                else if (compared > 0)
                {
                    break;
                }

                haveUpdate = await updatesEnumerator.MoveNextAsync();
            }

            if (!suppressCurrent)
            {
                while (haveContent)
                {
                    var currentContent = contentEnumerator.Current;
                    var compared = _options.SourceKeyComparer.Compare(currentContent.TargetKey, key);

                    if (compared == 0)
                    {
                        // Make it look like an upsert
                        yield return new SourceUpdate<SK, SV>
                        {
                            Key = currentContent.TargetKey,
                            Value = currentContent.Value
                        };
                    }
                    else if (compared > 0)
                    {
                        break;
                    }

                    haveContent = await contentEnumerator.MoveNextAsync();
                }
            }
        }
    }

    /// <summary>
    /// Reads from one or more pairs of streams. The streams in the pair are:
    /// 
    /// - Updates: containing parquet of <c>SourceUpdate</c> objects in source key order
    /// - Content: containing parquet of <c>ContentRecord</c> objects where the target
    /// types are the same as this production's source types.
    /// 
    /// The idea is to take the updates and content from a previous production and use
    /// them as input to this production.
    /// 
    /// The resulting sequence is suitable for passing to a <c>Update</c> call as the 
    /// <c>sourceUpdates</c> parameter.
    /// 
    /// </summary>
    /// <param name="sources">Streams containing parquet of SourceUpdate objects</param>
    /// <returns>A unified stream suitable for passing to an Update</returns>
    public async IAsyncEnumerable<SourceUpdate<SK, SV>> ReadSources(
        IReadOnlyCollection<(Stream Updates, Stream Content)> sources,
        [EnumeratorCancellation] CancellationToken cancellation)
    {
        foreach (var (updates, _) in sources)
        {
            updates.Position = 0;
        }

        // Generate a temporary stream of all updated keys across all sources
        var sourceKeyReaders = sources.Select(source => _options.Read<KeyOnly>(source.Updates, cancellation).Select(x => x.Key)).ToList();
        var affectedKeys = sourceKeyReaders[0].SortedMerge(_options.SourceKeyComparer, sourceKeyReaders.Skip(1).ToArray());

        using var affectedKeysStream = _options.CreateTemporaryStream(_options.LoggingPrefix + ".AffectedKeys");
        var affectedKeysWriter = new BufferedWriter<KeyOnly>(affectedKeysStream, _options.RowsPerGroup, _options.ParquetOptions);
        await affectedKeysWriter.AddRange(affectedKeys
            .DistinctUntilChanged(_options.SourceKeyComparer.ToEqualityComparer())
            .Select(x => new KeyOnly { Key = x }));
        await affectedKeysWriter.Finish();

        var distinctAffectedKeys = _options.Read<KeyOnly>(affectedKeysStream, cancellation).Select(x => x.Key!);

        var sourceReaders = sources.Select(source => ReadKeys(source.Updates, source.Content, distinctAffectedKeys, cancellation)).ToList();

        var comparer = Comparers.Build<SourceUpdate<SK, SV>>().By(x => x.Key, _options.SourceKeyComparer);

        SourceUpdate<SK, SV>? firstOfGroup = null;
        bool groupIsAllDeletions = false;

        await foreach (var update in sourceReaders[0].SortedMerge(comparer, sourceReaders.Skip(1).ToArray()))
        {
            var compared = firstOfGroup == null ? 1 : comparer.Compare(update, firstOfGroup);
            if (compared < 0)
            {
                throw new InvalidOperationException($"{_options.LoggingPrefix}: Update keys are not correctly ordered");
            }
            if (compared > 0) // New group
            {
                if (groupIsAllDeletions && firstOfGroup != null) // We only saw deletions for previous group
                {
                    yield return firstOfGroup;
                }

                firstOfGroup = update;
 
                if (update.Deletion)
                {
                    groupIsAllDeletions = true;
                    // Hold off on yielding until we know if there are any non-deletions
                }
                else
                {
                    groupIsAllDeletions = false;
                    yield return update;
                }
            }
            else // 2nd or later update for the same key
            {
                if (!update.Deletion)
                {
                    groupIsAllDeletions = false;
                    yield return update;
                }
            }
        }

        // Handle leftover
        if (groupIsAllDeletions && firstOfGroup != null)
        {
            yield return firstOfGroup;
        }
    }

    private async Task UpdateStream<R, I>(
        int rowsPerGroup,
        Stream previousStream,
        Stream updatedStream,
        IAsyncEnumerable<I> instructions,
        Func<IAsyncEnumerable<I>, IAsyncEnumerable<R>, IAsyncEnumerable<R>> reconcile,
        CancellationToken cancellation) where R : new()
    {
        var previousRecords = _options.Read<R>(previousStream, cancellation);

        var updatedContent = new BufferedWriter<R>(updatedStream, rowsPerGroup, _options.ParquetOptions);

        await updatedContent.AddRange(reconcile(instructions, previousRecords));
        await updatedContent.Finish();

        updatedStream.Position = 0;
    }
    
    private async Task GenerateInstructions(
        IAsyncEnumerable<KeyMapping<SK, TK>> keyMappings,
        IAsyncEnumerable<SourceUpdate<SK, SV>> sourceUpdates,
        InstructionsStorage<SK, TK, TV> instructions,
        Produce<SK, SV, TK, TV> production,
        CancellationToken cancellation)
    {
        await using var keyMappingsEnumerator = keyMappings.GetAsyncEnumerator(cancellation);
        var haveKeyMapping = await keyMappingsEnumerator.MoveNextAsync();

        await using var sourceUpdatesEnumerator = sourceUpdates.GetAsyncEnumerator(cancellation);
        var haveSourceUpdate = await sourceUpdatesEnumerator.MoveNextAsync();

        while (haveSourceUpdate)
        {
            // fast-forward to relevant keyMappings that tell us what needs to be deleted
            while (haveKeyMapping)
            {
                var compared = _options.SourceKeyComparer.Compare(
                    keyMappingsEnumerator.Current.SourceKey,
                    sourceUpdatesEnumerator.Current.Key);

                if (compared == 0)
                {
                    await instructions.Delete(keyMappingsEnumerator.Current.SourceKey,
                                              keyMappingsEnumerator.Current.TargetKey);
                }
                else if (compared > 0)
                {
                    break;
                }

                haveKeyMapping = await keyMappingsEnumerator.MoveNextAsync();
            }

            var currentKey = sourceUpdatesEnumerator.Current.Key;

            if (sourceUpdatesEnumerator.Current.Deletion)
            {
                haveSourceUpdate = await sourceUpdatesEnumerator.MoveNextAsync();

                if (haveSourceUpdate)
                {
                    var nextKeyCompared = _options.SourceKeyComparer.Compare(sourceUpdatesEnumerator.Current.Key, currentKey);

                    if (nextKeyCompared == 0)
                    {
                        throw new InvalidOperationException($"{_options.LoggingPrefix}: Deletion for key {currentKey} was followed by more updates for same key");
                    }

                    if (nextKeyCompared < 0)
                    {
                        throw new InvalidOperationException($"{_options.LoggingPrefix}: Update keys are not correctly ordered");
                    }
                }
            }
            else
            {
                var sequence = new SingleUseSequence<SourceUpdate<SK, SV>, SV>(
                    sourceUpdatesEnumerator,
                    x => x.Value!,
                    x => x.Value != null && _options.SourceKeyComparer.Compare(x.Key!, currentKey) != 0);

                await foreach (var projected in production(currentKey!, sequence))
                {
                    await instructions.Add(currentKey, projected.Key, projected.Value);
                }

                haveSourceUpdate = sequence.HasCurrent;
                if (haveSourceUpdate) 
                {
                    var nextKeyCompared = _options.SourceKeyComparer.Compare(sourceUpdatesEnumerator.Current.Key, currentKey);

                    if (nextKeyCompared == 0)
                    {
                        throw new InvalidOperationException($"{_options.LoggingPrefix}: Production did not consume all values for the same key");
                    }

                    if (nextKeyCompared < 0)
                    {
                        throw new InvalidOperationException($"{_options.LoggingPrefix}: Source update keys are not correctly ordered");
                    }
                }
            }
        }

        await instructions.Finish();
    }

    private async IAsyncEnumerable<KeyMapping<SK, TK>> ExecuteInstructionsOnMappings(
        IAsyncEnumerable<KeyMappingInstruction<SK, TK>> instructions,
        IAsyncEnumerable<KeyMapping<SK, TK>> previous)
    {
        var comparers = Comparers.Build<(KeyMappingInstruction<SK, TK> In, bool Instruction)>();

        var compareBothKeys = comparers.By(
                comparers.By(x => x.In.SourceKey, _options.SourceKeyComparer),
                comparers.By(x => x.In.TargetKey, _options.TargetKeyComparer));

        var compareForMerge = comparers.By(
                compareBothKeys,
                comparers.By(x => x.Instruction ? 0 : 1)); // update instructions go first

        var fromInstructions = instructions.Select(i => (In: i, Instruction: true));
        var fromExisting = previous.Select(r => (
            In: new KeyMappingInstruction<SK, TK> { SourceKey = r.SourceKey, TargetKey = r.TargetKey }, 
            Instruction: false));

        (KeyMappingInstruction<SK, TK> In, bool Instruction)? firstOfGroup = null;
        var discardExisting = false;

        KeyMapping<SK, TK> ToMapping(KeyMappingInstruction<SK, TK> i)
            => i.Deletion 
                ? throw new InvalidOperationException("Deletion cannot become key-mapping")
                : new KeyMapping<SK, TK> { SourceKey = i.SourceKey, TargetKey = i.TargetKey };
    
        await foreach (var next in fromInstructions.SortedMerge(compareForMerge, fromExisting))
        {
            var compared = firstOfGroup == null ? 1 : compareBothKeys.Compare(next, firstOfGroup.Value);
            if (compared < 0)
            {
                throw new InvalidOperationException($"{_options.LoggingPrefix}: Instructions are not correctly ordered");
            }

            if (compared != 0)
            {
                // Starting a new group. If there are instructions, they'll appear first
                // and we will know to discard existing mappings for the same keys

                firstOfGroup = next;
                if (next.Instruction)
                {
                    discardExisting = true;

                    if (!next.In.Deletion)
                    {
                        yield return ToMapping(next.In);
                    }                    
                }
                else // First is existing mapping so no instructions to modify it
                {
                    discardExisting = false;
                    yield return ToMapping(next.In);
                }
            }
            else if (next.Instruction || !discardExisting)
            {
                if (!next.In.Deletion)
                {
                    yield return ToMapping(next.In);
                }
            }
        }
    }

    enum ComparisonState
    {
        Equal,
        InstructionFirst,
        ExistingFirst,
        Exhausted,        
    }
    
    private async IAsyncEnumerable<ContentRecord<TK, SK, TV>> ExecuteInstructionsOnContent(
        IAsyncEnumerable<ContentInstruction<TK, SK, TV>> instructions,
        IAsyncEnumerable<ContentRecord<TK, SK, TV>> content,
        BufferedWriter<SourceUpdate<TK, TV>>? updates = null)
    {
        var instruction = await instructions.ToCursor();
        var existing = await content.ToCursor();

        var tkComparer = _options.TargetKeyComparer;
        var skComparer = _options.SourceKeyComparer;

        ComparisonState targets, sources;

        void CompareKeys(TK? itk, SK? isk, TK? xtk, SK? xsk)
        { 
            targets = tkComparer.Compare(itk, xtk) switch
            {
                > 0 => ComparisonState.ExistingFirst,
                < 0 => ComparisonState.InstructionFirst,
                _ => ComparisonState.Equal,
            };

            sources = skComparer.Compare(isk, xsk) switch
            {
                > 0 => ComparisonState.ExistingFirst,
                < 0 => ComparisonState.InstructionFirst,
                _ => ComparisonState.Equal,
            };
        }

        void CompareCursors()
        { 
            if (instruction.Valid && existing.Valid)
            {
                CompareKeys(
                    instruction.Value.TargetKey, instruction.Value.SourceKey,
                    existing.Value.TargetKey, existing.Value.SourceKey);
            }
            else
            {
                targets = sources = ComparisonState.Exhausted;
            }        
        }
        
        CompareCursors();

        var updateState = new PendingDeleteState<TK, TV>(updates, tkComparer);
        
        var instructionTargetKeys = new InstructionTargetKeys<TK, SK, TV>(instruction, tkComparer);
        instructionTargetKeys.Store();
        
        while (instruction.Valid && existing.Valid)
        {            
            if (targets == ComparisonState.Equal && sources == ComparisonState.Equal)
            {
                // Discard previous content with SameTargetAndSource
                do
                {
                    await existing.Next();
                    CompareCursors();
                }
                while (targets == ComparisonState.Equal && sources == ComparisonState.Equal);

                var tk = instruction.Value.TargetKey;
                var sk = instruction.Value.SourceKey;

                CompareKeys(instruction.Value.TargetKey, instruction.Value.SourceKey, tk, sk);

                // Process instructions
                while (targets == ComparisonState.Equal && sources == ComparisonState.Equal)
                {
                    if (instruction.Value.Deletion)
                    {
                        await updateState.SendDelete(instruction.Value.TargetKey);                                                        
                    }
                    else
                    {
                        yield return new ContentRecord<TK, SK, TV>
                        {
                            TargetKey = instruction.Value.TargetKey,
                            SourceKey = instruction.Value.SourceKey,
                            Value = instruction.Value.Value,
                        };
                        
                        await updateState.SendUpsert(instruction.Value.TargetKey, instruction.Value.Value);
                    }

                    await instruction.Next();
                    if (instruction.Valid)
                    {
                        CompareKeys(instruction.Value.TargetKey, instruction.Value.SourceKey, tk, sk);
                    }
                    else
                    {
                        targets = sources = ComparisonState.Exhausted;
                    }
                }
            }
            else if (targets == ComparisonState.InstructionFirst ||
                    (targets == ComparisonState.Equal && 
                     sources == ComparisonState.InstructionFirst))
            {
                // Instruction for a (TK, SK) with no existing content, so not
                // expecting any deletes(?)
                if (instruction.Value.Deletion)
                {
                    throw new InvalidOperationException("Unexpected deletion for keys with no existing content");
                }
                
                yield return new ContentRecord<TK, SK, TV>
                {
                    TargetKey = instruction.Value.TargetKey,
                    SourceKey = instruction.Value.SourceKey,
                    Value = instruction.Value.Value,
                };

                await updateState.SendUpsert(instruction.Value.TargetKey, instruction.Value.Value);

                await instruction.Next();
            }
            else
            {
                yield return existing.Value;

                // if same target key as either the previous or next instruction, send upserts
                if (instructionTargetKeys.Matches(existing.Value.TargetKey))
                {
                    await updateState.SendUpsert(existing.Value.TargetKey, existing.Value.Value);
                }

                await existing.Next();
            }

            CompareCursors();
            instructionTargetKeys.Store();
        }

        // Left-overs
        while (instruction.Valid)
        {
            // Instruction for a (TK, SK) with no existing content, so not 
            // expecting any deletes(?)
            if (instruction.Value.Deletion)
            {
                throw new InvalidOperationException("Unexpected deletion for keys with no existing content");
            }
            
            yield return new ContentRecord<TK, SK, TV>
            {
                TargetKey = instruction.Value.TargetKey,
                SourceKey = instruction.Value.SourceKey,
                Value = instruction.Value.Value,
            };

            await updateState.SendUpsert(instruction.Value.TargetKey, instruction.Value.Value);

            await instruction.Next();            
        }

        while (existing.Valid)
        {
            yield return existing.Value;

            // if same target key as either the previous or next instruction, send upserts
            if (instructionTargetKeys.Matches(existing.Value.TargetKey))
            {
                await updateState.SendUpsert(existing.Value.TargetKey, existing.Value.Value);
            }

            await existing.Next();
        }

        await updateState.Finish();
    }
}
