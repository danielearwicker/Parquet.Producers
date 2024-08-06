using System.Diagnostics;
using FluentAssertions;
using Parquet.Producers.Types;
using Parquet.Producers.Util;
using Parquet.Serialization;

namespace Parquet.Producers.Tests;

public sealed class ParquetProductionTests : IDisposable
{
    private interface IDataStore<K, V>
    {
        public Stream Content { get; }
        public Stream Updates { get; }
    }

    private class DataStore<SK, SV, TK, TV>(
        Produce<SK, SV, TK, TV> produce,
        ParquetProducerPlatformOptions platform,
        ParquetProducerOptions<SK, TK, TV>? options = null)
        : IDataStore<TK, TV>
    {
        public readonly MemoryStream PreviousMappings = new();
        public readonly MemoryStream UpdatedMappings = new();
        public readonly MemoryStream PreviousContent = new();
        public readonly MemoryStream UpdatedContent = new();
        public readonly MemoryStream UpdatesMade = new();

        public Stream Content => UpdatedContent;
        public Stream Updates => UpdatesMade;

        public readonly ParquetProduction<SK, SV, TK, TV> Production = new(platform, options);
        
        private static void SwapStreams(Stream previous, Stream updated)
        {
            previous.SetLength(0);
            updated.Position = 0;
            updated.CopyTo(previous);
            updated.SetLength(0);
        }

        public Task Update(IAsyncEnumerable<SourceUpdate<SK, SV>> updates)
        {
            SwapStreams(PreviousMappings, UpdatedMappings);
            SwapStreams(PreviousContent, UpdatedContent);

            return Production.Update(
                PreviousMappings, UpdatedMappings,
                PreviousContent, UpdatedContent,
                updates, produce,
                UpdatesMade);
        }

        public Task Update(params SourceUpdate<SK, SV>[] updates)
            => Update(updates.ToAsyncEnumerable());

        public Task UpdateFrom(params IDataStore<SK, SV>[] sources)
            => Update(ReadSources(sources));

        public IAsyncEnumerable<SourceUpdate<SK, SV>> ReadSources(params IDataStore<SK, SV>[] sources)
            => Production.ReadSources(sources.Select(x => (x.Updates, x.Content)).ToArray(), default);

        public async Task<IEnumerable<ContentRecord<TK, SK, TV>>> ReadContent()
            => await ParquetSerializer.DeserializeAllAsync<ContentRecord<TK, SK, TV>>(
                UpdatedContent).ToListAsync();

        public async Task<IEnumerable<KeyMapping<SK, TK>>> ReadKeyMappings()
            => await ParquetSerializer.DeserializeAllAsync<KeyMapping<SK, TK>>(
                UpdatedMappings).ToListAsync();

        public async Task<IEnumerable<SourceUpdate<TK, TV>>> ReadUpdates()
            => await ParquetSerializer.DeserializeAllAsync<SourceUpdate<TK, TV>>(
                UpdatesMade).ToListAsync();

        public async Task Trace()
        {
            Debug.WriteLine("----------- content -------------");
            foreach (var item in await ReadContent())
            {
                Debug.WriteLine($"{item.TargetKey} [{item.SourceKey}] {item.Value}");
            }

            Debug.WriteLine("---------- mappings -------------");
            foreach (var item in await ReadKeyMappings())
            {
                Debug.WriteLine($"{item.SourceKey} [{item.TargetKey}]");
            }
        }

        public async Task AssertContents(params (TK, SK, TV)[] expected)
            => (await ReadContent())
                .Select(x => (x.TargetKey, x.SourceKey, x.Value))
                .Should().BeEquivalentTo(expected, o => o.WithStrictOrdering());

        public async Task AssertKeyMappings(params (SK, TK)[] expected)
            => (await ReadKeyMappings())
                .Select(x => (x.SourceKey, x.TargetKey))
                .Should().BeEquivalentTo(expected, o => o.WithStrictOrdering());

        public async Task AssertUpdates(params (TK, TV, SourceUpdateType)[] expected)
            => (await ReadUpdates())
                .Select(x => (x.Key, x.Value, x.Type))
                .Should().BeEquivalentTo(expected, o => o.WithStrictOrdering());

        public async Task AssertSources(IDataStore<SK, SV>[] sources, params (SK, SV, SourceUpdateType)[] expected)
            => (await ReadSources(sources).ToListAsync())
                .Select(x => (x.Key, x.Value, x.Type))
                .Should().BeEquivalentTo(expected, o => o.WithStrictOrdering());
    }

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
        var data = new DataStore<int, StuffIn, int, StuffOut>(
            ProjectStuff,
            new ParquetProducerPlatformOptions
            {
                CreateTemporaryStream = CreateTemporaryStream
            });

        await data.Update(
            new() { Key = 1, Value = new StuffIn { FirstName = "Randy", LastName = "Newman", Copies = 1 } },
            new() { Key = 2, Value = new StuffIn { FirstName = "Gary", LastName = "Oldman", Copies = 1 } },
            new() { Key = 2, Value = new StuffIn { FirstName = "Gary", LastName = "Newman", Copies = 1 } },
            new() { Key = 3, Value = new StuffIn { FirstName = "Randy", LastName = "Oldman", Copies = 1 } });

        // Contents are sorted by (TargetKey, SourceKey)
        await data.AssertContents(
            (1, 1, new StuffOut { Id = 1, FirstFullName = "Randy Newman", Copy = 1 }),
            (1, 3, new StuffOut { Id = 3, FirstFullName = "Randy Oldman", Copy = 1 }),
            (2, 2, new StuffOut { Id = 2, FirstFullName = "Gary Oldman", Copy = 1 }));

        // KeyMappings are sorted by SourceKey
        await data.AssertKeyMappings(
            (1, 1),
            (2, 2),
            (3, 1));

        // Generate multiple outputs from one input (replacing source key 1)
        await data.Update(new SourceUpdate<int, StuffIn>
        {
            Key = 1,
            Value = new StuffIn { FirstName = "Randy", LastName = "Newman", Copies = 3 }
        });

        await data.AssertContents(
            (1, 1, new StuffOut { Id = 1, FirstFullName = "Randy Newman", Copy = 1 }),
            (1, 1, new StuffOut { Id = 1, FirstFullName = "Randy Newman", Copy = 2 }),
            (1, 1, new StuffOut { Id = 1, FirstFullName = "Randy Newman", Copy = 3 }),
            (1, 3, new StuffOut { Id = 3, FirstFullName = "Randy Oldman", Copy = 1 }),
            (2, 2, new StuffOut { Id = 2, FirstFullName = "Gary Oldman", Copy = 1 }));

        await data.AssertKeyMappings(
            (1, 1),
            (1, 1),
            (1, 1),
            (2, 2),
            (3, 1));

        // Change source key 3 to have 2 records, and thus SK 2 & 3's will contribute to target key 2
        await data.Update(
            new() { Key = 3, Value = new StuffIn { FirstName = "Silly", LastName = "Oldman", Copies = 1 } },
            new() { Key = 3, Value = new StuffIn { FirstName = "Randy", LastName = "Oldman", Copies = 1 } });

        await data.AssertContents(
            (1, 1, new StuffOut { Id = 1, FirstFullName = "Randy Newman", Copy = 1 }),
            (1, 1, new StuffOut { Id = 1, FirstFullName = "Randy Newman", Copy = 2 }),
            (1, 1, new StuffOut { Id = 1, FirstFullName = "Randy Newman", Copy = 3 }),
            (2, 2, new StuffOut { Id = 2, FirstFullName = "Gary Oldman", Copy = 1 }),
            (2, 3, new StuffOut { Id = 3, FirstFullName = "Silly Oldman", Copy = 1 }));

        await data.AssertKeyMappings(
            (1, 1),
            (1, 1),
            (1, 1),
            (2, 2),
            (3, 2));

        // Delete source key 2
        await data.Update(new SourceUpdate<int, StuffIn>() { Key = 2, Type = SourceUpdateType.Delete });

        await data.AssertContents(
            (1, 1, new StuffOut { Id = 1, FirstFullName = "Randy Newman", Copy = 1 }),
            (1, 1, new StuffOut { Id = 1, FirstFullName = "Randy Newman", Copy = 2 }),
            (1, 1, new StuffOut { Id = 1, FirstFullName = "Randy Newman", Copy = 3 }),
            (2, 3, new StuffOut { Id = 3, FirstFullName = "Silly Oldman", Copy = 1 }));

        await data.AssertKeyMappings(
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
        var platform = new ParquetProducerPlatformOptions
        {
            CreateTemporaryStream = CreateTemporaryStream
        };
        
        var phrasesById = new DataStore<int, string, int, string>(
            SimpleText_Identity, 
            platform);
        
        var booksById = new DataStore<int, string, int, string>(
            SimpleText_Identity,
            platform);
        
        var idsByWord = new DataStore<int, string, string, int>(
            SimpleText_SplitIntoWords, 
            platform);

        var wordCounts = new DataStore<string, int, int, string>(
            SimpleText_CountWords,
            platform,
            new() 
            {
                TargetKeyComparer = Comparer<int>.Default.Reverse(),
            });

        await phrasesById.Update(
            new() { Key = 1, Value = "the quick brown fox" },
            new() { Key = 2, Value = "jumps over the lazy dog" },
            new() { Key = 3, Value = "sometimes a dog is brown" },
            new() { Key = 4, Value = "brown is my favourite colour" });

        await phrasesById.AssertContents(
            (1, 1, "the quick brown fox"),
            (2, 2, "jumps over the lazy dog"),
            (3, 3, "sometimes a dog is brown"),
            (4, 4, "brown is my favourite colour"));

        await phrasesById.AssertKeyMappings(
            (1, 1),
            (2, 2),
            (3, 3),
            (4, 4));

        await phrasesById.AssertUpdates(
            (1, "the quick brown fox", SourceUpdateType.Add),
            (2, "jumps over the lazy dog", SourceUpdateType.Add),
            (3, "sometimes a dog is brown", SourceUpdateType.Add),
            (4, "brown is my favourite colour", SourceUpdateType.Add));

        await booksById.Update(
            new() { Key = 1, Value = "the brain police" },
            new() { Key = 2, Value = "sometimes the fox is lazy" },
            new() { Key = 3, Value = "the mystery at dog hall" });

        await booksById.AssertContents(
            (1, 1, "the brain police"),
            (2, 2, "sometimes the fox is lazy"),
            (3, 3, "the mystery at dog hall")
        );

        await booksById.AssertKeyMappings(
            (1, 1),
            (2, 2),
            (3, 3));

        await booksById.AssertUpdates(
            (1, "the brain police", SourceUpdateType.Add),
            (2, "sometimes the fox is lazy", SourceUpdateType.Add),
            (3, "the mystery at dog hall", SourceUpdateType.Add)
        );

        await idsByWord.UpdateFrom(phrasesById, booksById);

        await idsByWord.AssertContents(
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
        
        await wordCounts.UpdateFrom(idsByWord);

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

        await wordCounts.AssertContents(expected.Select(x => (x.Count, x.Word, x.Word)).ToArray());

        await phrasesById.Update(
            new SourceUpdate<int, string>() { Key = 2, Type = SourceUpdateType.Delete }); // "jumps over the lazy dog"

        await phrasesById.AssertContents(
            (1, 1, "the quick brown fox"),
            (3, 3, "sometimes a dog is brown"),
            (4, 4, "brown is my favourite colour"));

        await phrasesById.AssertKeyMappings(
            (1, 1),
            (3, 3),
            (4, 4));

        await phrasesById.AssertUpdates(
            (2, default!, SourceUpdateType.Delete));

        booksById.UpdatesMade.SetLength(0);

        await idsByWord.AssertSources([phrasesById, booksById], 
            (2, "sometimes the fox is lazy", SourceUpdateType.Update));

        await idsByWord.UpdateFrom(phrasesById, booksById);

        await idsByWord.AssertContents(
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
        await idsByWord.AssertUpdates(
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

        await wordCounts.AssertSources([idsByWord], 
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

        await wordCounts.UpdateFrom(idsByWord);

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

        await wordCounts.AssertContents(expected.Select(x => (x.Count, x.Word, x.Word)).ToArray());
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
        var platform = new ParquetProducerPlatformOptions
        {
            CreateTemporaryStream = CreateTemporaryStream
        };
        
        var nextId = 1;

        var words = new DataStore<int, string, string, WordId>(
            PreservingValues_Generate, 
            platform,
            new()
            {
                PreserveKeyValues = (target, example) => target.Id = example?.Id ?? nextId++
            });
        
        await words.Update(
            new() { Key = 1, Value = "dog" },
            new() { Key = 1, Value = "budgie" },
            new() { Key = 2, Value = "dog" },
            new() { Key = 2, Value = "cat" },
            new() { Key = 3, Value = "eagle" },
            new() { Key = 3, Value = "dog" },
            new() { Key = 3, Value = "cat" });

        await words.AssertUpdates(
            ("budgie", new WordId { Id = 1 }, SourceUpdateType.Add),
            ("cat", new WordId { Id = 2 }, SourceUpdateType.Add),
            ("cat", new WordId { Id = 2 }, SourceUpdateType.Update),
            ("dog", new WordId { Id = 3 }, SourceUpdateType.Add),
            ("dog", new WordId { Id = 3 }, SourceUpdateType.Update),
            ("dog", new WordId { Id = 3 }, SourceUpdateType.Update),
            ("eagle", new WordId { Id = 4 }, SourceUpdateType.Add)
        );

        await words.AssertContents(
            ("budgie", 1, new WordId { Id = 1 }),
            ("cat", 2, new WordId { Id = 2 }),
            ("cat", 3, new WordId { Id = 2 }),
            ("dog", 1, new WordId { Id = 3 }),
            ("dog", 2, new WordId { Id = 3 }),
            ("dog", 3, new WordId { Id = 3 }),
            ("eagle", 3, new WordId { Id = 4 }));

        
        await words.Update(
            new() { Key = 2, Value = "frog" },
            new() { Key = 2, Value = "eagle" },
            new() { Key = 2, Value = "ant" });

        await words.AssertUpdates(
            ("ant", new WordId { Id = 5 }, SourceUpdateType.Add),      // source 2, new target key
            ("cat", new WordId { Id = 2 }, SourceUpdateType.Update),   // source 3
            ("dog", new WordId { Id = 3 }, SourceUpdateType.Update),   // source 1
            ("dog", new WordId { Id = 3 }, SourceUpdateType.Update),   // source 3
            ("eagle", new WordId { Id = 4 }, SourceUpdateType.Update), // source 2
            ("eagle", new WordId { Id = 4 }, SourceUpdateType.Update), // source 3
            ("frog", new WordId { Id = 6 }, SourceUpdateType.Add)      // source 2, new target key
        );

        await words.AssertContents(
            ("ant", 2, new WordId { Id = 5 }),
            ("budgie", 1, new WordId { Id = 1 }),
            ("cat", 3, new WordId { Id = 2 }),
            ("dog", 1, new WordId { Id = 3 }),
            ("dog", 3, new WordId { Id = 3 }),
            ("eagle", 2, new WordId { Id = 4 }),
            ("eagle", 3, new WordId { Id = 4 }),
            ("frog", 2, new WordId { Id = 6 }));

        await words.Update(
            new() { Key = 1, Value = "dog" },
            new() { Key = 1, Value = "frog" });

        await words.AssertUpdates(
            ("budgie", default!, SourceUpdateType.Delete),
            ("dog", new WordId { Id = 3 }, SourceUpdateType.Update),
            ("dog", new WordId { Id = 3 }, SourceUpdateType.Update),
            ("frog", new WordId { Id = 6 }, SourceUpdateType.Update),
            ("frog", new WordId { Id = 6 }, SourceUpdateType.Update)
        );

        await words.AssertContents(
            ("ant", 2, new WordId { Id = 5 }),
            ("cat", 3, new WordId { Id = 2 }),
            ("dog", 1, new WordId { Id = 3 }),
            ("dog", 3, new WordId { Id = 3 }),
            ("eagle", 2, new WordId { Id = 4 }),
            ("eagle", 3, new WordId { Id = 4 }),
            ("frog", 1, new WordId { Id = 6 }),
            ("frog", 2, new WordId { Id = 6 }));

        await words.Update(
            new() { Key = 1, Value = "dog" },
            new() { Key = 1, Value = "budgie" });

        await words.AssertUpdates(
            ("budgie", new WordId { Id = 7 }, SourceUpdateType.Add), // source 1, new target key
            ("dog", new WordId { Id = 3 }, SourceUpdateType.Update), // source 1
            ("dog", new WordId { Id = 3 }, SourceUpdateType.Update), // source 3            
            ("frog", new WordId { Id = 6 }, SourceUpdateType.Update) // source 2
        );

        await words.AssertContents(
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
