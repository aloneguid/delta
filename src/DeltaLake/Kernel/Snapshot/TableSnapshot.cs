using System.Runtime.Intrinsics.X86;
using DeltaLake.Kernel.Engine;
using DeltaLake.Kernel.Internal.Actions;
using DeltaLake.Kernel.Internal.Replay;
using Stowage;

namespace DeltaLake.Kernel.Snapshot {

    /// <summary>
    /// Snapshot represents the consistent state (a.k.a. a snapshot consistency) in a specific version of the table.
    /// </summary>
    public class TableSnapshot {

        public TableSnapshot(IOPath dataPath,
            LogSegment logSegment,
            LogReplay logReplay,
            Protocol protocol,
            Metadata metadata) {
            LogPath = dataPath.Combine("_delta_log");
            DataPath = dataPath;
            LogSegment = logSegment;
            LogReplay = logReplay;
            Protocol = protocol;
            Metadata = metadata;
            Version = logSegment.Version;

        }

        public IOPath LogPath { get; }
        public IOPath DataPath { get; }
        public LogSegment LogSegment { get; }
        public LogReplay LogReplay { get; }
        public Protocol Protocol { get; }
        public Metadata Metadata { get; }
        public long Version { get; }

        /// <summary>
        /// Returns the commit coordinator client handler based on the table metadata in this snapshot.
        /// </summary>
        /// <param name="engine"></param>
        /// <returns>the commit coordinator client handler for this snapshot or empty if the metadata is not configured to use the commit coordinator.</returns>
        public TableCommitCoordinatorClientHandler? GetTableCommitCoordinatorClientHandlerOpt(IEngine engine) {
            return null;
        }
    }
}
