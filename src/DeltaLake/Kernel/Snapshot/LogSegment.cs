using Stowage;

namespace DeltaLake.Kernel.Snapshot {

    /// <summary>
    /// Provides information around which files in the transaction log need to be read to create the given version of the log.
    /// </summary>
    public class LogSegment {

        public static LogSegment Empty(IOPath path) =>
            new LogSegment(path, -1, new List<IOEntry>(), new List<IOEntry>(), null, DateTimeOffset.MinValue);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="logPath">The path to the _delta_log directory</param>
        /// <param name="version">The Snapshot version to generate</param>
        /// <param name="deltas">The delta commit files (.json) to read</param>
        /// <param name="checkpoints">The checkpoint file(s) to read</param>
        /// <param name="checkpointVersion">The checkpoint version used to start replay</param>
        /// <param name="lastCommitTimestamp">The "unadjusted" timestamp of the last commit within this segment.
        /// By unadjusted, we mean that the commit timestamps may not necessarily be monotonically increasing
        /// for the commits within this segment.</param>
        public LogSegment(IOPath logPath,
            long version,
            List<IOEntry> deltas,
            List<IOEntry> checkpoints,
            long? checkpointVersion,
            DateTimeOffset lastCommitTimestamp) {
            LogPath = logPath;
            Version = version;
            Deltas = deltas;
            Checkpoints = checkpoints;
            CheckpointVersion = checkpointVersion;
            LastCommitTimestamp = lastCommitTimestamp;


        }

        public IOPath LogPath { get; }
        public long Version { get; }
        public List<IOEntry> Deltas { get; }
        public List<IOEntry> Checkpoints { get; }
        public long? CheckpointVersion { get; }
        public DateTimeOffset LastCommitTimestamp { get; }

        /// <summary>
        /// All deltas (.json) and checkpoint (.checkpoint.parquet) files in this LogSegment, with no ordering guarantees.
        /// </summary>
        public IEnumerable<IOEntry> AllFiles => Checkpoints.Concat(Deltas);

        /// <summary>
        /// All deltas (.json) and checkpoint (.checkpoint.parquet) files in this LogSegment, sorted in
        /// reverse(00012.json, 00011.json, 00010.checkpoint.parquet) order.
        /// </summary>
        public IEnumerable<IOEntry> AllFilesReversed => AllFiles.OrderByDescending(f => f.Name);
    }
}
