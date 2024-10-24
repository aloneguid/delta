using Apache.Arrow;
using DeltaLake;
using DeltaLake.Log.Actions;
using Stowage;
using Xunit;

namespace DeltaLake.Test {
    public class DeltaLogTest {

        private readonly IFileStorage _storage;

        public DeltaLogTest() {
            _storage = Files.Of.LocalDisk(Path.GetFullPath(Path.Combine("data")));
        }

        [Fact]
        public async Task GoldenLogTestAsync() {
            Table table = await Table.OpenAsync(_storage, new IOPath("golden", "data-reader-array-primitives"));

            Assert.Equal(5, table.Log.Actions.Count);

            // 0
            var a0 = (CommitInfoAction)table.Log.Actions[0];
            Assert.Equal(DeltaAction.CommitInfo, a0.DeltaAction);

            // 1
            var a1 = (ProtocolEvolutionAction)table.Log.Actions[1];
            Assert.Equal(DeltaAction.Protocol, a1.DeltaAction);
            Assert.Equal(1, a1.MinReaderVersion);
            Assert.Equal(2, a1.MinWriterVersion);

            // 2
            var a2 = (ChangeMetadataAction)table.Log.Actions[2];
            Assert.Equal(DeltaAction.Metadata, a2.DeltaAction);

            // 3
            var a3 = (AddFileAction)table.Log.Actions[3];
            Assert.Equal(DeltaAction.AddFile, a3.DeltaAction);

            // 4
            var a4 = (AddFileAction)table.Log.Actions[4];
            Assert.Equal(DeltaAction.AddFile, a4.DeltaAction);
        }

        [Fact]
        public async Task SimpleTableTestAsync() {
            Table table = await Table.OpenAsync(_storage, new IOPath("simple_table"));

            Assert.Equal(74, table.Log.Actions.Count);

            IReadOnlyCollection<string> files = table.Log.GetFiles();

            // check that we have 5 files at the end after replaying all actions
            Assert.Equal(5, files.Count);
            Assert.Equal([
                "part-00000-2befed33-c358-4768-a43c-3eda0d2a499d-c000.snappy.parquet",
                "part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet",
                "part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet",
                "part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet",
                "part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet"],
                files.Order().ToList());
        }
    }
}