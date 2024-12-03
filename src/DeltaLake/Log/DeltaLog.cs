using System.Text.Json;
using DeltaLake.Log.Actions;
using Parquet.Serialization;
using Stowage;

namespace DeltaLake.Log {

    public class LogEntry {
        public LogEntry(IOEntry commitFile) {
            Entry = commitFile;
            Version = ParseVersion(commitFile.Path);
        }

        public IOEntry Entry { get; }

        public long Version { get; }

        public bool IsJson => Entry.Name.EndsWith(".json");

        public bool IsClassicCheckpoint => Entry.Name.EndsWith(".checkpoint.parquet");

        public bool IsLastCheckpoint => Entry.Name == DeltaLog.LastCheckpointFileName;

        public bool ShouldIgnore => Entry.Name.EndsWith(".crc");

        private static long ParseVersion(IOPath path) {
            string vs = path.Name;
            int idx = vs.IndexOf('.');
            if(idx != -1) {
                vs = vs.Substring(0, idx);
            }

            long.TryParse(vs, out long v);
            return v;
        }

        public override string ToString() => $"{Version} {Entry.Name}";
    }

    /// <summary>
    /// Implements delta log protocol as per https://github.com/delta-io/delta/blob/master/PROTOCOL.md#delta-log-entries
    /// </summary>
    public class DeltaLog {

        public const string DeltaLogDirName = "_delta_log";
        public const string LastCheckpointFileName = "_last_checkpoint";

        private readonly IFileStorage _storage;
        private readonly IOPath _location;
        //private readonly List<IOEntry> _entries = new List<IOEntry>();
        //private readonly List<Action> _actions = new List<Action>();

        public DeltaLog(IFileStorage storage, IOPath location) {
            _storage = storage;
            _location = location;
        }

        private async Task<IReadOnlyCollection<LogEntry>> ListLogEntries() {
            // Delta log files are stored as JSON in a directory at the root of the table named _delta_log,
            // and together with checkpoints make up the log of all changes that have occurred to a table.
            IReadOnlyCollection<IOEntry> logFiles = await _storage.Ls(_location.Combine(DeltaLogDirName) + "/");
            var logEntries = logFiles
                .Select(f => new LogEntry(f))
                .Where(e => !e.ShouldIgnore)
                .OrderBy(e => e.Version)
                .ToList();

            return logEntries;
        }

        private static IReadOnlyCollection<LogEntry> CompactLogEntries(IReadOnlyCollection<LogEntry> logEntries) {
            // find max checkpoint
            long maxCheckpointVersion = logEntries
                .Where(e => e.IsClassicCheckpoint)
                .Select(e => e.Version)
                .DefaultIfEmpty(-1)
                .Max();

            // filter out json entries with version less than max checkpoint
            return logEntries
                .Where(e => e.Version >= maxCheckpointVersion)
                .ToList();
        }

        private async Task<LogCommit> ReadJsonAsCommit(LogEntry entry) {
            var commit = new LogCommit(entry);
            string? content = await _storage.ReadText(entry.Entry.Path);
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

        private async Task<IEnumerable<LogCommit>> ReadParquetAsCommits(LogEntry entry) {
            var result = new List<LogCommit>();

            // read into memory stream (these files should be tiny)
            var src = new MemoryStream();
            using(Stream? s = await _storage.OpenRead(entry.Entry.Path)) {
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
            IReadOnlyCollection<LogEntry> entries = await ListLogEntries();
            entries = CompactLogEntries(entries);

            foreach(LogEntry entry in entries) {

                if(entry.IsJson) {
                    commits.Add(await ReadJsonAsCommit(entry));
                } else if(entry.IsLastCheckpoint) {
                    // "last checkpoint" file
                } else if(entry.IsClassicCheckpoint) {
                    // "classic" checkpoint file
                    commits.AddRange(await ReadParquetAsCommits(entry));
                } else {
                    throw new NotImplementedException(entry.ToString());
                }
            }

            return commits;
        }
    }
}