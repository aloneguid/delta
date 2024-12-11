using DeltaLake.Kernel.Engine;
using DeltaLake.Kernel.Exceptions;
using DeltaLake.Kernel.Internal.Actions;
using DeltaLake.Kernel.Snapshot;
using Action = DeltaLake.Kernel.Internal.Actions.Action;
using Stowage;
using System;

namespace DeltaLake.Kernel.Internal.Replay {

    /// <summary>
    ///  Replays a history of actions, resolving them to produce the current state of the table. The
    ///  protocol for resolution is as follows:
    ///  - The most recent {@code AddFile} and accompanying metadata for any `(path, dv id)` tuple wins.
    ///  - {@code RemoveFile} deletes a corresponding AddFile. A {@code RemoveFile} "corresponds" to the AddFile that matches both the
    ///    parquet file URI *and* the deletion vector's URI (if any).
    ///  - The most recent {@code Metadata} wins.
    ///  - The most recent {@code Protocol} version wins.
    ///  - For each `(path, dv id)` tuple, this class should always output only one {@code FileAction}
    ///    (either { @code AddFile} or {@code RemoveFile})
    /// </summary>
    public class LogReplay {
        public IOPath DataPath { get; }
        public LogSegment LogSegment { get; }
        public Protocol Protocol { get; }
        public Metadata Metadata { get; }

        private LogReplay(IOPath dataPath, LogSegment logSegment, Protocol protocol, Metadata metadata) {
            DataPath = dataPath;
            LogSegment = logSegment;
            Protocol = protocol;
            Metadata = metadata;
        }

        public static async Task<LogReplay> CreateAsync(IOPath logPath,
            IOPath dataPath,
            long snapshotVersion,
            IEngine engine,
            LogSegment logSegment,
            SnapshotHint? snapshotHint) {

            AssertLogFilesBelongToTable(logPath, logSegment.AllFiles);

            (Protocol, Metadata) pm = await LoadTableProtocolAndMetadata(snapshotHint, snapshotVersion, logSegment, engine, dataPath);
            await LoadDomainMetadataMap();

            return new LogReplay(dataPath, logSegment, pm.Item1, pm.Item2);
        }

        /// <summary>
        /// Returns the latest Protocol and Metadata from the delta files in the `logSegment`. Does *not*
        /// validate that this delta-kernel connector understands the table at that protocol.
        /// 
        /// Uses the `snapshotHint` to bound how many delta files it reads. i.e. we only need to read
        /// delta files newer than the hint to search for any new P & M. If we don't find them, we can just
        /// use the P and/or M from the hint.
        /// </summary>
        /// <param name="snapshotHint"></param>
        /// <param name="snapshotVersion"></param>
        /// <param name="logSegment"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        private static async Task<(Protocol, Metadata)> LoadTableProtocolAndMetadata(
            SnapshotHint? snapshotHint, long snapshotVersion,
            LogSegment logSegment, IEngine engine, IOPath dataPath) {

            // Exit early if the hint already has the info we need
            if(snapshotHint != null && snapshotHint.Version == snapshotVersion) {
                return (snapshotHint.Protocol, snapshotHint.Metadata);
            }

            Protocol? protocol = null;
            Metadata? metadata = null;

            await foreach(Action action in new ActionsIterator(logSegment.AllFilesReversed, engine)) {

                switch(action.DeltaAction) {
                    case ActionType.Metadata:
                        if(metadata == null) {
                            metadata = (Metadata)action;
                            if(protocol != null) {
                                TableFeatures.ValidateReadSupportedTable(protocol, dataPath, metadata);
                                return (protocol, metadata);
                            }
                        }
                        break;
                    case ActionType.Protocol:
                        if(protocol == null) {
                            protocol = (Protocol)action;
                        }
                        break;
                }

                // Found latest protocol and metadata, exit this loop
                if(protocol != null && metadata != null)
                    break;
            }

            // Since we haven't returned, at least one of P or M is null.
            // Note: Suppose the hint is at version N. We check the hint eagerly at N + 1 so
            // that we don't read or open any files at version N.
            if(protocol == null || metadata == null) {
                if(snapshotHint != null && logSegment.Version == snapshotHint.Version + 1) {
                    if(protocol == null) {
                        protocol = snapshotHint.Protocol;
                    }
                    if(metadata == null) {
                        metadata = snapshotHint.Metadata;
                    }
                    return (protocol, metadata);
                }
            }

            if(protocol == null) {
                throw new KernelException($"No protocol found at version {logSegment.Version}");
            }

            throw new KernelException($"No protocol found at version {logSegment.Version}");
        }

        private static async Task LoadDomainMetadataMap() {
            //throw new NotImplementedException();
        }

        /// <summary>
        /// Verifies that a set of delta or checkpoint files to be read actually belongs to this table.
        /// Visible only for testing.
        /// </summary>
        /// <param name="logPath"></param>
        /// <param name="allFiles"></param>
        /// <exception cref="KernelException"></exception>
        public static void AssertLogFilesBelongToTable(IOPath logPath, IEnumerable<IOEntry> allFiles) {
            foreach(IOEntry file in allFiles) {
                if(!file.Path.Full.StartsWith(logPath.Full)) {
                    throw new KernelException(
                        "File ("
                            + file
                            + ") doesn't belong in the "
                            + "transaction log at "
                            + logPath
                            + ".");
                }
            }
        }

    }
}
