using DeltaLake;
using DeltaLake.Log;
using DeltaLake.Log.Actions;
using Stowage;
using Xunit;
using Action = DeltaLake.Log.Actions.Action;

namespace DeltaLake.Test {
    public class DeltaLogTest {

        private readonly IFileStorage _storage;

        const int ArtistRowCount = 275;

        public DeltaLogTest() {
            _storage = Files.Of.LocalDisk(Path.GetFullPath(Path.Combine("data")));
        }

        /// <summary>
        /// Entire Artist table written in one go with a single commit.
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task ArtistSimple() {
            Table table = new Table(_storage, new IOPath("chinook", "artist.simple"));

            IReadOnlyCollection<LogCommit> commits = await table.Log.ReadHistoryAsync();

            // should be only a single commit
            Assert.Single(commits);

            List<Action> actions = commits.First().Actions;
            // 0
            var a0 = (CommitInfoAction)actions[0];
            Assert.Equal(DeltaAction.CommitInfo, a0.DeltaAction);

            // 1
            var a1 = (ProtocolEvolutionAction)actions[1];
            Assert.Equal(DeltaAction.Protocol, a1.DeltaAction);
            Assert.Equal(1, a1.MinReaderVersion);
            Assert.Equal(2, a1.MinWriterVersion);

            // 2
            var a2 = (ChangeMetadataAction)actions[2];
            Assert.Equal(DeltaAction.Metadata, a2.DeltaAction);

            // 3
            var a3 = (AddFileAction)actions[3];
            Assert.Equal(DeltaAction.AddFile, a3.DeltaAction);

            // file set

            IReadOnlyCollection<string> files = await table.GetFilesAsync();
            Assert.Single(files);
            Assert.Equal([
                "part-00000-df960eb7-f439-480a-b59b-c145d2da0a1d-c000.snappy.parquet"],
                files.Order().ToList());
        }

        /// <summary>
        /// Artist table written in multiple commits, each commit adding a batch of 20 rows.
        /// This simulates a trickle load with tiny microbatch commits, like in a streaming ETL job.
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task ArtistTrickle() {
            Table table = new Table(_storage, new IOPath("chinook", "artist.trickle"));

            IReadOnlyCollection<LogCommit> commits = await table.Log.ReadHistoryAsync();

            // there shoudl be exactly 4 commits (1 checkpointed and 3 json)
            Assert.Equal(4, commits.Count);

            // file set

            IReadOnlyCollection<string> files = await table.GetFilesAsync();
            // there should be exactly 14 files (batches of 20 rows)
            Assert.Equal(14, files.Count);
        }
    }
}