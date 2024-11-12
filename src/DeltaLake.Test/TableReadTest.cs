using Parquet.Schema;
using Parquet.Serialization;
using Stowage;
using Xunit;

namespace DeltaLake.Test {

    public class Artist {
        public int? ArtistId { get; set; }

        public string? Name { get; set; }
    }

    public class TableReadTest {
        private readonly IFileStorage _storage;

        public TableReadTest() {
            _storage = Files.Of.LocalDisk(Path.GetFullPath(Path.Combine("data")));
        }

        [Fact]
        public async Task ArtistSimple() {
            Table table = await Table.OpenAsync(_storage, new IOPath("chinook", "artist.simple"));

            Assert.Single(table.DataFiles);

            using Stream parquetStream = await table.OpenSeekableStreamAsync(table.DataFiles.First());

            IList<Artist> artists = await ParquetSerializer.DeserializeAsync<Artist>(parquetStream);

            Assert.Equal(275, artists.Count);
        }
    }
}
