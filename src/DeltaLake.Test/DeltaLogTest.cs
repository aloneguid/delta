using DeltaLake;
using DeltaLake.Log;
using DeltaLake.Log.Actions;
using Stowage;
using Xunit;
using A = DeltaLake.Log.Actions;

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

            List<A.Action> actions = commits.First().Actions;
            // 0
            var a0 = (A.CommitInfo)actions[0];
            Assert.Equal(ActionType.CommitInfo, a0.DeltaAction);

            // 1
            var a1 = (A.ProtocolEvolution)actions[1];
            Assert.Equal(ActionType.Protocol, a1.DeltaAction);
            Assert.Equal(1, a1.MinReaderVersion);
            Assert.Equal(2, a1.MinWriterVersion);

            // 2
            var a2 = (A.Metadata)actions[2];
            Assert.Equal(ActionType.Metadata, a2.DeltaAction);

            // 3
            var a3 = (A.AddFile)actions[3];
            Assert.Equal(ActionType.AddFile, a3.DeltaAction);

            // file set

            IReadOnlyCollection<DataFile> files = await table.GetDataFilesAsync();
            Assert.Single(files);
            Assert.Equal([
                "/chinook/artist.simple/part-00000-df960eb7-f439-480a-b59b-c145d2da0a1d-c000.snappy.parquet"],
                files.Select(f => f.Path).Order().ToList());
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

            IReadOnlyCollection<DataFile> files = await table.GetDataFilesAsync();
            // there should be exactly 14 files (batches of 20 rows)
            Assert.Equal(14, files.Count);
        }

        [Fact]
        public async Task TrackPartitinedByMediaTypeId() {
            var table = new Table(_storage, new IOPath("chinook", "track.partitioned.mediatypeid"));

            IReadOnlyCollection<LogCommit> commits = await table.Log.ReadHistoryAsync();

            IReadOnlyCollection<DataFile> files = await table.GetDataFilesAsync();

            Assert.Single(commits);

            // there should be 1 partition definion and 5 partition values
            var partitionNames = files.SelectMany(f => f.PartitionValues.Keys).Distinct().ToList();
            Assert.Single(partitionNames);
            Assert.Equal("MediaTypeId", partitionNames[0]);

            var partitionValues = files.SelectMany(f => f.PartitionValues.Values).Distinct().ToList();
            Assert.Equal(5, partitionValues.Count);
            Assert.Equal(new[] { "1", "2", "3", "4", "5" }, partitionValues);
        }
    }
}