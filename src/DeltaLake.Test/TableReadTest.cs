using Parquet.Schema;
using Parquet.Serialization;
using Stowage;
using Xunit;

namespace DeltaLake.Test {
    public class TableReadTest {
        private readonly IFileStorage _storage;

        public TableReadTest() {
            _storage = Files.Of.LocalDisk(Path.GetFullPath(Path.Combine("data")));
        }

        [Fact]
        public async Task ArtistSimple() {
            Table table = new Table(_storage, new IOPath("chinook", "artist.simple"));

            IReadOnlyCollection<DataFile> dataFiles = await table.GetDataFilesAsync();

            Assert.Single(dataFiles);

            using Stream parquetStream = await table.OpenSeekableStreamAsync(dataFiles.First());

            ParquetSerializer.UntypedResult ur = await ParquetSerializer.DeserializeAsync(parquetStream);

            Assert.Equivalent(new ParquetSchema(), ur.Schema);
        }
    }
}
