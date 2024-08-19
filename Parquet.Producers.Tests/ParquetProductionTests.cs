using System.Diagnostics;
using Apache.Arrow.Types;
using FluentAssertions;
using Parquet.Producers.Parquet;
using Parquet.Producers.Serialization;
using Parquet.Producers.TestCommand;
using Parquet.Producers.Types;
using Parquet.Producers.Util;

namespace Parquet.Producers.Tests;

public sealed class ParquetProductionTests : IDisposable
{
    public class MemoryPersistentStreams : IPersistentStreams
    {
        private readonly Dictionary<string, byte[]> _blobs = [];

        private static string FullName(string name, PersistentStreamType type, int version)
            => $"{name}-{type}-{version}";

        public async Task<Stream> OpenRead(string name, PersistentStreamType type, int version)
        {
            var stream = new MemoryStream();

            if (_blobs.TryGetValue(FullName(name, type, version), out var original))
            {                
                await stream.WriteAsync(original);
                stream.Position = 0; 
            }
            
            return stream;
        }
        
        public async Task Upload(string name, PersistentStreamType type, int version, Stream content, CancellationToken cancellation)
        {
            var copy = new MemoryStream();
            await content.CopyToAsync(copy, cancellation);

            _blobs[FullName(name, type, version)] = copy.ToArray();
        }
    }

    private static readonly ISerializationFormat _format = new ParquetSerializationFormat(new ParquetOptions());

    private static async Task<List<T>> Read<T>(IPersistentStreams storage, string name, PersistentStreamType type, int version)
        where T : new()
    {
        using var stream = await storage.OpenRead(name, type, version);
        if (stream.Length == 0) return [];

        var reader = await _format.Read<T>(stream);
        var result = new List<T>();

        for (var i = 0; i < reader.RowGroupCount; i++)
        {
            var group = await reader.Get(i, default);

            foreach (var item in group)
            {
                result.Add(item);
            }
        }

        return result;
    }

    private static async Task AssertContents<SK, TK, TV>(IPersistentStreams storage, string name, int version, params (TK, SK, TV)[] expected)
        => (await Read<ContentRecord<TK, SK, TV>>(storage, name, PersistentStreamType.Content, version))
            .Select(x => (x.TargetKey, x.SourceKey, x.Value))
            .Should().BeEquivalentTo(expected, o => o.WithStrictOrdering());

    private static async Task AssertKeyMappings<SK, TK>(IPersistentStreams storage, string name, int version, params (SK, TK)[] expected)
        => (await Read<KeyMapping<SK, TK>>(storage, name, PersistentStreamType.KeyMappings, version))
            .Select(x => (x.SourceKey, x.TargetKey))
            .Should().BeEquivalentTo(expected, o => o.WithStrictOrdering());

    private static async Task AssertUpdates<TK, TV>(IPersistentStreams storage, string name, int version, params (TK, TV, SourceUpdateType)[] expected)
        => (await Read<SourceUpdate<TK, TV>>(storage, name, PersistentStreamType.Update, version))
            .Select(x => (x.Key, x.Value, x.Type))
            .Should().BeEquivalentTo(expected, o => o.WithStrictOrdering());

    // private async Task AssertSources<SK, SV>(IProducer<SK, SV>[] sources, params (SK, SV, SourceUpdateType)[] expected)
    //     => (await ReadSources(sources).ToListAsync())
    //         .Select(x => (x.Key, x.Value, x.Type))
    //         .Should().BeEquivalentTo(expected, o => o.WithStrictOrdering());


    private class TestTempStream(string label) : MemoryStream
    {
        public bool Disposed { get; private set; }

        public readonly string Label = label;

        public override void Close()
        {
            Debug.WriteLine("Disposed " + Label);
            base.Close();
            Disposed = true;
        }
    }

    private readonly List<TestTempStream> _tempStream = [];

    private Stream CreateTemporaryStream(string label)
    {
        Debug.WriteLine(label);
        var stream = new TestTempStream(label);
        _tempStream.Add(stream);
        return stream;
    }

    public void Dispose()
    {
        foreach (var stream in _tempStream)
        {
            stream.Disposed.Should().BeTrue(stream.Label);
        }
    }

    public class StuffIn
    {
        public string FirstName { get; set; } = string.Empty;

        public string LastName { get; set; } = string.Empty;

        public int Copies { get; set; }
    }

    public class StuffOut
    {
        public int Id { get; set; }

        public string FirstFullName { get; set; } = string.Empty;

        public int Copy { get; set; }
    }

    static async IAsyncEnumerable<(int, StuffOut)> ProjectStuff(int id, IAsyncEnumerable<StuffIn> values)
    {
        var count = 0;
        var copies = 0;
        string firstFullName = string.Empty;
        await foreach (var value in values)
        {
            if (firstFullName == string.Empty)
            {
                firstFullName = $"{value.FirstName} {value.LastName}";
            }

            count++;

            copies = Math.Max(copies, value.Copies);
        }

        for (var i = 1; i <= copies; i++)
        {
            yield return (count, new() { Id = id, FirstFullName = firstFullName, Copy = i });
        }
    }

    [Test]
    public async Task ValidFromEmpty()
    {
        var storage = new MemoryPersistentStreams();

        var data = new Producer<int, StuffIn, int, StuffOut>(
            storage,
            "a",
            ProjectStuff,
            new ParquetProducerPlatformOptions
            {
                CreateTemporaryStream = CreateTemporaryStream
            });

        await data.UpdateAll(
            new SourceUpdate<int, StuffIn>[] 
            {
                new() { Key = 1, Value = new StuffIn { FirstName = "Randy", LastName = "Newman", Copies = 1 } },
                new() { Key = 2, Value = new StuffIn { FirstName = "Gary", LastName = "Oldman", Copies = 1 } },
                new() { Key = 2, Value = new StuffIn { FirstName = "Gary", LastName = "Newman", Copies = 1 } },
                new() { Key = 3, Value = new StuffIn { FirstName = "Randy", LastName = "Oldman", Copies = 1 } }
            }
            .ToAsyncEnumerable(), 0, default);

        // Contents are sorted by (TargetKey, SourceKey)
        await AssertContents(storage, "a", 1,
            (1, 1, new StuffOut { Id = 1, FirstFullName = "Randy Newman", Copy = 1 }),
            (1, 3, new StuffOut { Id = 3, FirstFullName = "Randy Oldman", Copy = 1 }),
            (2, 2, new StuffOut { Id = 2, FirstFullName = "Gary Oldman", Copy = 1 }));

        // KeyMappings are sorted by SourceKey
        await AssertKeyMappings(storage, "a", 1,
            (1, 1),
            (2, 2),
            (3, 1));

        // Generate multiple outputs from one input (replacing source key 1)
        await data.UpdateAll(
            new SourceUpdate<int, StuffIn>[]
            {
                new() 
                {
                    Key = 1,
                    Value = new StuffIn { FirstName = "Randy", LastName = "Newman", Copies = 3 }
                }
            }
            .ToAsyncEnumerable(), 1, default);

        await AssertContents(storage, "a", 2,
            (1, 1, new StuffOut { Id = 1, FirstFullName = "Randy Newman", Copy = 1 }),
            (1, 1, new StuffOut { Id = 1, FirstFullName = "Randy Newman", Copy = 2 }),
            (1, 1, new StuffOut { Id = 1, FirstFullName = "Randy Newman", Copy = 3 }),
            (1, 3, new StuffOut { Id = 3, FirstFullName = "Randy Oldman", Copy = 1 }),
            (2, 2, new StuffOut { Id = 2, FirstFullName = "Gary Oldman", Copy = 1 }));

        await AssertKeyMappings(storage, "a", 2,
            (1, 1),
            (1, 1),
            (1, 1),
            (2, 2),
            (3, 1));

        // Change source key 3 to have 2 records, and thus SK 2 & 3's will contribute to target key 2
        await data.UpdateAll(
            new SourceUpdate<int, StuffIn>[]
            {
                new() { Key = 3, Value = new StuffIn { FirstName = "Silly", LastName = "Oldman", Copies = 1 } },
                new() { Key = 3, Value = new StuffIn { FirstName = "Randy", LastName = "Oldman", Copies = 1 } }
            }
            .ToAsyncEnumerable(), 2, default);

        await AssertContents(storage, "a", 3,
            (1, 1, new StuffOut { Id = 1, FirstFullName = "Randy Newman", Copy = 1 }),
            (1, 1, new StuffOut { Id = 1, FirstFullName = "Randy Newman", Copy = 2 }),
            (1, 1, new StuffOut { Id = 1, FirstFullName = "Randy Newman", Copy = 3 }),
            (2, 2, new StuffOut { Id = 2, FirstFullName = "Gary Oldman", Copy = 1 }),
            (2, 3, new StuffOut { Id = 3, FirstFullName = "Silly Oldman", Copy = 1 }));

        await AssertKeyMappings(storage, "a", 3,
            (1, 1),
            (1, 1),
            (1, 1),
            (2, 2),
            (3, 2));

        // Delete source key 2
        await data.UpdateAll(
            new SourceUpdate<int, StuffIn>[] 
            {
                 new () { Key = 2, Type = SourceUpdateType.Delete } 
            }
            .ToAsyncEnumerable(), 3, default);

        await AssertContents(storage, "a", 4,
            (1, 1, new StuffOut { Id = 1, FirstFullName = "Randy Newman", Copy = 1 }),
            (1, 1, new StuffOut { Id = 1, FirstFullName = "Randy Newman", Copy = 2 }),
            (1, 1, new StuffOut { Id = 1, FirstFullName = "Randy Newman", Copy = 3 }),
            (2, 3, new StuffOut { Id = 3, FirstFullName = "Silly Oldman", Copy = 1 }));

        await AssertKeyMappings(storage, "a", 4,
            (1, 1),
            (1, 1),
            (1, 1),
            (3, 2));
    }

    private static async IAsyncEnumerable<(int, string)> SimpleText_Identity(int id, IAsyncEnumerable<string> values)
    {
        await foreach (var value in values)
        {
            yield return (id, value);
        }
    }

    private static async IAsyncEnumerable<(string, int)> SimpleText_SplitIntoWords(int id, IAsyncEnumerable<string> values)
    {
        await foreach (var value in values)
        {
            foreach (var word in value.Split(' '))
            {
                yield return (word, id);
            }
        }
    }

    private static async IAsyncEnumerable<(int, string)> SimpleText_CountWords(string word, IAsyncEnumerable<int> ids)
    {
        yield return (await ids.CountAsync(), word);
    }

    [Test]
    public async Task WordCounting()
    {
        var storage = new MemoryPersistentStreams();

        var platform = new ParquetProducerPlatformOptions
        {
            CreateTemporaryStream = CreateTemporaryStream
        };
        
        var phrasesById = new Producer<int, string, int, string>(
            storage,
            "phrasesById",
            SimpleText_Identity, 
            platform);
        
        var booksById = new Producer<int, string, int, string>(
            storage,
            "booksById",
            SimpleText_Identity,
            platform);
        
        var idsByWord = phrasesById.Produces("idsByWord", SimpleText_SplitIntoWords, null, booksById);
        
        var wordCounts = idsByWord.Produces("wordCounts", SimpleText_CountWords, new() 
            {
                TargetKeyComparer = Comparer<int>.Default.Reverse(),
            });

        await phrasesById.Update(
            new SourceUpdate<int, string>[]
            {
                new() { Key = 1, Value = "the quick brown fox" },
                new() { Key = 2, Value = "jumps over the lazy dog" },
                new() { Key = 3, Value = "sometimes a dog is brown" },
                new() { Key = 4, Value = "brown is my favourite colour" }
            }
            .ToAsyncEnumerable(), 0, default);

        await AssertContents(storage, "phrasesById", 1,
            (1, 1, "the quick brown fox"),
            (2, 2, "jumps over the lazy dog"),
            (3, 3, "sometimes a dog is brown"),
            (4, 4, "brown is my favourite colour"));

        await AssertKeyMappings(storage, "phrasesById", 1,
            (1, 1),
            (2, 2),
            (3, 3),
            (4, 4));

        await AssertUpdates(storage, "phrasesById", 1,
            (1, "the quick brown fox", SourceUpdateType.Add),
            (2, "jumps over the lazy dog", SourceUpdateType.Add),
            (3, "sometimes a dog is brown", SourceUpdateType.Add),
            (4, "brown is my favourite colour", SourceUpdateType.Add));

        await booksById.Update(
            new SourceUpdate<int, string>[]
            {
                new() { Key = 1, Value = "the brain police" },
                new() { Key = 2, Value = "sometimes the fox is lazy" },
                new() { Key = 3, Value = "the mystery at dog hall" }
            }
            .ToAsyncEnumerable(), 0, default);

        await AssertContents(storage, "booksById", 1,
            (1, 1, "the brain police"),
            (2, 2, "sometimes the fox is lazy"),
            (3, 3, "the mystery at dog hall")
        );

        await AssertKeyMappings(storage, "booksById", 1,
            (1, 1),
            (2, 2),
            (3, 3));

        await AssertUpdates(storage, "booksById", 1,
            (1, "the brain police", SourceUpdateType.Add),
            (2, "sometimes the fox is lazy", SourceUpdateType.Add),
            (3, "the mystery at dog hall", SourceUpdateType.Add)
        );

        await booksById.UpdateTargets(1, default);

        await AssertContents(storage, "idsByWord", 1,
            ("a", 3, 3),
            ("at", 3, 3),
            ("brain", 1, 1),
            ("brown", 1, 1),
            ("brown", 3, 3),
            ("brown", 4, 4),
            ("colour", 4, 4),
            ("dog", 2, 2),
            ("dog", 3, 3),            
            ("dog", 3, 3),
            ("favourite", 4, 4),
            ("fox", 1, 1),
            ("fox", 2, 2),
            ("hall", 3, 3),
            ("is", 2, 2),
            ("is", 3, 3),
            ("is", 4, 4),
            ("jumps", 2, 2),
            ("lazy", 2, 2),
            ("lazy", 2, 2),
            ("my", 4, 4),
            ("mystery", 3, 3),
            ("over", 2, 2),
            ("police", 1, 1),
            ("quick", 1, 1),
            ("sometimes", 2, 2),
            ("sometimes", 3, 3),
            ("the", 1, 1),
            ("the", 1, 1),
            ("the", 2, 2),
            ("the", 2, 2),
            ("the", 3, 3));
        
        var expected = new (string Word, int Count)[]
        {
            ("the", 5),
            ("brown", 3),
            ("dog", 3),
            ("is", 3),
            ("fox", 2),
            ("lazy", 2),
            ("sometimes", 2),
            ("a", 1),
            ("at", 1),
            ("brain", 1),
            ("colour", 1),
            ("favourite", 1),
            ("hall", 1),
            ("jumps", 1),
            ("my", 1),
            ("mystery", 1),
            ("over", 1), 
            ("police", 1), 
            ("quick", 1)
        };

        await AssertContents(storage, "wordCounts", 1, expected.Select(x => (x.Count, x.Word, x.Word)).ToArray());

        await phrasesById.Update(
            new SourceUpdate<int, string>[]
            {
                new() { Key = 2, Type = SourceUpdateType.Delete }
            }
            .ToAsyncEnumerable(), 1, default); // "jumps over the lazy dog"

        await AssertContents(storage, "phrasesById", 2,
            (1, 1, "the quick brown fox"),
            (3, 3, "sometimes a dog is brown"),
            (4, 4, "brown is my favourite colour"));

        await AssertKeyMappings(storage, "phrasesById", 2,
            (1, 1),
            (3, 3),
            (4, 4));

        await AssertUpdates<int, string>(storage, "phrasesById", 2,
            (2, default!, SourceUpdateType.Delete));

        // Don't need to do anything to booksById, and it will therefore appear to have
        // an empty list of updates inversion 2, which is correct.

        // await idsByWord.AssertSources([phrasesById, booksById], 
        //     (2, "sometimes the fox is lazy", SourceUpdateType.Update));

        await booksById.UpdateTargets(2, default);

        await AssertContents(storage, "idsByWord", 2,
            ("a", 3, 3),
            ("at", 3, 3),
            ("brain", 1, 1),
            ("brown", 1, 1),
            ("brown", 3, 3),
            ("brown", 4, 4),
            ("colour", 4, 4),
            ("dog", 3, 3),            
            ("dog", 3, 3),
            ("favourite", 4, 4),
            ("fox", 1, 1),
            ("fox", 2, 2),
            ("hall", 3, 3),
            ("is", 2, 2),
            ("is", 3, 3),
            ("is", 4, 4),
            ("lazy", 2, 2),
            ("my", 4, 4),
            ("mystery", 3, 3),
            ("police", 1, 1),
            ("quick", 1, 1),
            ("sometimes", 2, 2),
            ("sometimes", 3, 3),
            ("the", 1, 1),
            ("the", 1, 1),
            ("the", 2, 2),
            ("the", 3, 3));

            // "jumps over the lazy dog" - will appear as deletions
            // "sometimes the fox is lazy" - will appear as (unnecessary) upserts
        await AssertUpdates(storage, "idsByWord", 2,
            ("dog", 3, SourceUpdateType.Update),
            ("dog", 3, SourceUpdateType.Update),
            ("fox", 1, SourceUpdateType.Update),
            ("fox", 2, SourceUpdateType.Update),
            ("is", 2, SourceUpdateType.Update),
            ("is", 3, SourceUpdateType.Update),
            ("is", 4, SourceUpdateType.Update),
            ("jumps", 0, SourceUpdateType.Delete),
            ("lazy", 2, SourceUpdateType.Update),
            ("over", 0, SourceUpdateType.Delete),
            ("sometimes", 2, SourceUpdateType.Update),
            ("sometimes", 3, SourceUpdateType.Update),
            ("the", 1, SourceUpdateType.Update),
            ("the", 1, SourceUpdateType.Update),
            ("the", 2, SourceUpdateType.Update),
            ("the", 3, SourceUpdateType.Update));

        // await wordCounts.AssertSources([idsByWord], 
        //     ("dog", 3, SourceUpdateType.Update),
        //     ("dog", 3, SourceUpdateType.Update),
        //     ("fox", 1, SourceUpdateType.Update),
        //     ("fox", 2, SourceUpdateType.Update),
        //     ("is", 2, SourceUpdateType.Update),
        //     ("is", 3, SourceUpdateType.Update),
        //     ("is", 4, SourceUpdateType.Update),
        //     ("jumps", 0, SourceUpdateType.Delete),
        //     ("lazy", 2, SourceUpdateType.Update),
        //     ("over", 0, SourceUpdateType.Delete),
        //     ("sometimes", 2, SourceUpdateType.Update),
        //     ("sometimes", 3, SourceUpdateType.Update),
        //     ("the", 1, SourceUpdateType.Update),
        //     ("the", 1, SourceUpdateType.Update),
        //     ("the", 2, SourceUpdateType.Update),
        //     ("the", 3, SourceUpdateType.Update));

        expected = 
        [
            ("the", 4),
            ("brown", 3),
            ("is", 3),            
            ("dog", 2),
            ("fox", 2),
            ("sometimes", 2),
            ("a", 1),
            ("at", 1),
            ("brain", 1),
            ("colour", 1),
            ("favourite", 1),
            ("hall", 1),
            ("lazy", 1),
            ("my", 1),
            ("mystery", 1), 
            ("police", 1), 
            ("quick", 1)
        ];

        await AssertContents(storage, "wordCounts", 2,
            expected.Select(x => (x.Count, x.Word, x.Word)).ToArray());
    }

    public class WordId
    {
        public int Id { get; set; }
    }

    private static async IAsyncEnumerable<(string, WordId)> PreservingValues_Generate(int _, IAsyncEnumerable<string> words)
    {
        await foreach (var word in words)
        {
            yield return (word, new());
        }
    }

    [Test]
    public async Task PreservingValues()
    {
        var storage = new MemoryPersistentStreams();

        var nextId = 1;

        var words = new Producer<int, string, string, WordId>(
            storage,
            "words",
            PreservingValues_Generate,
            new ParquetProducerPlatformOptions
            {
                CreateTemporaryStream = CreateTemporaryStream
            },
            new()
            {
                PreserveKeyValues = (target, example) => target.Id = example?.Id ?? nextId++
            });

        await words.Update(
            new SourceUpdate<int, string>[]
            {
                new() { Key = 1, Value = "dog" },
                new() { Key = 1, Value = "budgie" },
                new() { Key = 2, Value = "dog" },
                new() { Key = 2, Value = "cat" },
                new() { Key = 3, Value = "eagle" },
                new() { Key = 3, Value = "dog" },
                new() { Key = 3, Value = "cat" }
            }
            .ToAsyncEnumerable(), 0, default);

        await AssertUpdates(storage, "words", 1,
            ("budgie", new WordId { Id = 1 }, SourceUpdateType.Add),
            ("cat", new WordId { Id = 2 }, SourceUpdateType.Add),
            ("cat", new WordId { Id = 2 }, SourceUpdateType.Update),
            ("dog", new WordId { Id = 3 }, SourceUpdateType.Add),
            ("dog", new WordId { Id = 3 }, SourceUpdateType.Update),
            ("dog", new WordId { Id = 3 }, SourceUpdateType.Update),
            ("eagle", new WordId { Id = 4 }, SourceUpdateType.Add)
        );

        await AssertContents(storage, "words", 1,
            ("budgie", 1, new WordId { Id = 1 }),
            ("cat", 2, new WordId { Id = 2 }),
            ("cat", 3, new WordId { Id = 2 }),
            ("dog", 1, new WordId { Id = 3 }),
            ("dog", 2, new WordId { Id = 3 }),
            ("dog", 3, new WordId { Id = 3 }),
            ("eagle", 3, new WordId { Id = 4 }));
       
       await words.Update(
            new SourceUpdate<int, string>[]
            {
                new() { Key = 2, Value = "frog" },
                new() { Key = 2, Value = "eagle" },
                new() { Key = 2, Value = "ant" }
            }
            .ToAsyncEnumerable(), 1, default);

        await AssertUpdates(storage, "words", 2,
            ("ant", new WordId { Id = 5 }, SourceUpdateType.Add),      // source 2, new target key
            ("cat", new WordId { Id = 2 }, SourceUpdateType.Update),   // source 3
            ("dog", new WordId { Id = 3 }, SourceUpdateType.Update),   // source 1
            ("dog", new WordId { Id = 3 }, SourceUpdateType.Update),   // source 3
            ("eagle", new WordId { Id = 4 }, SourceUpdateType.Update), // source 2
            ("eagle", new WordId { Id = 4 }, SourceUpdateType.Update), // source 3
            ("frog", new WordId { Id = 6 }, SourceUpdateType.Add)      // source 2, new target key
        );

        await AssertContents(storage, "words", 2,
            ("ant", 2, new WordId { Id = 5 }),
            ("budgie", 1, new WordId { Id = 1 }),
            ("cat", 3, new WordId { Id = 2 }),
            ("dog", 1, new WordId { Id = 3 }),
            ("dog", 3, new WordId { Id = 3 }),
            ("eagle", 2, new WordId { Id = 4 }),
            ("eagle", 3, new WordId { Id = 4 }),
            ("frog", 2, new WordId { Id = 6 }));

        await words.Update(
            new SourceUpdate<int, string>[]
            {
                new() { Key = 1, Value = "dog" },
                new() { Key = 1, Value = "frog" }
            }
            .ToAsyncEnumerable(), 2, default);

        await AssertUpdates(storage, "words", 3,
            ("budgie", default!, SourceUpdateType.Delete),
            ("dog", new WordId { Id = 3 }, SourceUpdateType.Update),
            ("dog", new WordId { Id = 3 }, SourceUpdateType.Update),
            ("frog", new WordId { Id = 6 }, SourceUpdateType.Update),
            ("frog", new WordId { Id = 6 }, SourceUpdateType.Update)
        );

        await AssertContents(storage, "words", 3,
            ("ant", 2, new WordId { Id = 5 }),
            ("cat", 3, new WordId { Id = 2 }),
            ("dog", 1, new WordId { Id = 3 }),
            ("dog", 3, new WordId { Id = 3 }),
            ("eagle", 2, new WordId { Id = 4 }),
            ("eagle", 3, new WordId { Id = 4 }),
            ("frog", 1, new WordId { Id = 6 }),
            ("frog", 2, new WordId { Id = 6 }));

        await words.Update(
            new SourceUpdate<int, string>[]
            {
                new() { Key = 1, Value = "dog" },
                new() { Key = 1, Value = "budgie" }
            }
            .ToAsyncEnumerable(), 3, default);

        await AssertUpdates(storage, "words", 4,
            ("budgie", new WordId { Id = 7 }, SourceUpdateType.Add), // source 1, new target key
            ("dog", new WordId { Id = 3 }, SourceUpdateType.Update), // source 1
            ("dog", new WordId { Id = 3 }, SourceUpdateType.Update), // source 3            
            ("frog", new WordId { Id = 6 }, SourceUpdateType.Update) // source 2
        );

        await AssertContents(storage, "words", 4,
            ("ant", 2, new WordId { Id = 5 }),
            ("budgie", 1, new WordId { Id = 7 }),
            ("cat", 3, new WordId { Id = 2 }),
            ("dog", 1, new WordId { Id = 3 }),
            ("dog", 3, new WordId { Id = 3 }),
            ("eagle", 2, new WordId { Id = 4 }),
            ("eagle", 3, new WordId { Id = 4 }),
            ("frog", 2, new WordId { Id = 6 }));
    }
}
