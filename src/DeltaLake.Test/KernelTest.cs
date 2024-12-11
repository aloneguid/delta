using DeltaLake.Kernel.Engine;
using DeltaLake.Kernel.Snapshot;
using KTable = DeltaLake.Kernel.Table;
using Stowage;
using Xunit;

namespace DeltaLake.Test {
    /// <summary>
    /// Following the tutorials at https://docs.delta.io/3.2.1/delta-kernel-java.html#read-a-delta-table-in-a-single-process
    /// </summary>
    public class KernelTest {
        private readonly IFileStorage _storage;

        public KernelTest() {
            _storage = Files.Of.LocalDisk(Path.GetFullPath(Path.Combine("data")));
        }

        [Fact]
        public async Task FullScan() {
            var engine = new DefaultEngine(_storage);
            var path = new IOPath("chinook", "artist.simple");
            var table = new KTable(path);
            TableSnapshot snapshot = await table.GetLatestSnapshotAsync(engine);

            Assert.Equal(0, snapshot.Version);
        }
    }
}
