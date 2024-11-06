using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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
        }
    }
}
