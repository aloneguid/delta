using System.Text.Json;
using DeltaLake.Log.Actions;
using Parquet.Serialization;
using Stowage;

namespace DeltaLake.Log {

    /// <summary>
    /// Implements delta log protocol as per https://github.com/delta-io/delta/blob/master/PROTOCOL.md#delta-log-entries
    /// </summary>
    public class DeltaLog {

        public const string DeltaLogDirName = "_delta_log";
        const string LastCheckpointFileName = "_last_checkpoint";

        private readonly IFileStorage _storage;
        private readonly IOPath _location;
        //private readonly List<IOEntry> _entries = new List<IOEntry>();
        //private readonly List<Action> _actions = new List<Action>();

        public DeltaLog(IFileStorage storage, IOPath location) {
            _storage = storage;
            _location = location;
        }

        private static bool IsJsonFile(IOEntry entry) => entry.Name.EndsWith(".json");

        private static bool IsClassicCheckpointFile(IOEntry entry) => entry.Name.EndsWith(".checkpoint.parquet");

        private static bool IsLastCheckpointFile(IOEntry entry) => entry.Name == LastCheckpointFileName;

        private static bool IgnoreFile(IOEntry entry) => entry.Name.EndsWith(".crc");

        /// <summary>
        /// 
        /// </summary>
        /// <param name="compact">When true, the minimum about of files will be returned</param>
        /// <returns></returns>
        private async Task<IReadOnlyCollection<IOEntry>> ListLogEntries(bool compact = true) {
            // Delta log files are stored as JSON in a directory at the root of the table named _delta_log,
            // and together with checkpoints make up the log of all changes that have occurred to a table.
            IReadOnlyCollection<IOEntry> logEntries = await _storage.Ls(_location.Combine(DeltaLogDirName) + "/");
            logEntries = logEntries
                .Where(e => !IgnoreFile(e))
                .OrderBy(e => e.Name)
                .ToList();

            if(compact) {
                // find max checkpoint
                int maxCheckpointVersion = logEntries
                    .Where(IsClassicCheckpointFile)
                    .Select(e => int.Parse(e.Name.Split('.')[0]))
                    .DefaultIfEmpty(-1)
                    .Max();

                // filter out json entries with version less than max checkpoint
                logEntries = logEntries
                    .Where(e => !IsJsonFile(e) || int.Parse(e.Name.Split('.')[0]) > maxCheckpointVersion)
                    .ToList();
            }

            return logEntries;
        }

        private async Task<LogCommit> ReadJsonAsCommit(IOEntry entry) {
            var commit = new LogCommit(entry);
            string? content = await _storage.ReadText(entry.Path);
            if(content == null)
                throw new InvalidOperationException();
            foreach(string jsonLineRaw in content.Split('\n')) {
                string jsonLine = jsonLineRaw.Trim();
                if(string.IsNullOrEmpty(jsonLine))
                    continue;

                CommitLine? cl = JsonSerializer.Deserialize<CommitLine>(jsonLine);

                if(cl == null)
                    throw new ApplicationException("unparseable action: " + jsonLine);

                commit.Actions.Add(cl.ToAction());

            }
            return commit;
        }

        private async Task<IEnumerable<LogCommit>> ReadParquetAsCommits(IOEntry entry) {
            var result = new List<LogCommit>();

            // read into memory stream (these files should be tiny)
            var src = new MemoryStream();
            using(Stream? s = await _storage.OpenRead(entry.Path)) {
                if(s == null) {
                    throw new InvalidOperationException();
                }

                await s.CopyToAsync(src);
                src.Position = 0;
            }

            // read parquet file
            IList<CommitLine> commits = await ParquetSerializer.DeserializeAsync<CommitLine>(src);

            var commit = new LogCommit(entry);
            foreach(CommitLine cl in commits) {
                commit.Actions.Add(cl.ToAction());
            }

            result.Add(commit);
            return result;
        }

        public async Task<IReadOnlyCollection<LogCommit>> ReadHistoryAsync() {

            var commits = new List<LogCommit>();
            IReadOnlyCollection<IOEntry> entries = await ListLogEntries(true);

            foreach(IOEntry entry in entries) {

                if(IsJsonFile(entry)) {
                    commits.Add(await ReadJsonAsCommit(entry));
                } else if(IsLastCheckpointFile(entry)) {
                    // "last checkpoint" file
                } else if(IsClassicCheckpointFile(entry)) {
                    // "classic" checkpoint file
                    commits.AddRange(await ReadParquetAsCommits(entry));
                } else {
                    throw new NotImplementedException(entry.Name);
                }
            }

            return commits;
        }
    }
}