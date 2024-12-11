using DeltaLake.Kernel.Engine;
using DeltaLake.Kernel.Internal.Checkpoints;
using Stowage;
using System.Diagnostics;
using DeltaLake.Kernel.Engine.CoordinatedCommits;
using DeltaLake.Kernel.Exceptions;
using System.Text;
using DeltaLake.Kernel.Internal.Replay;
using DeltaLake.Kernel.Internal.Actions;

namespace DeltaLake.Kernel.Snapshot {

    /// <summary>
    /// Contains summary information of a <see cref="TableSnapshot">.
    /// </summary>
    /// <param name="Version"></param>
    /// <param name="Protocol"></param>
    /// <param name="Metadata"></param>
    public record SnapshotHint(long Version, Protocol Protocol, Metadata Metadata);

    public class SnapshotManager {

        // The latest {@link SnapshotHint} for this table. The initial value inside the AtomicReference is null.
        private SnapshotHint? latestSnapshotHint;

        public SnapshotManager(IOPath logPath, IOPath tablePath) {
            LogPath = logPath;
            TablePath = tablePath;
        }

        public IOPath LogPath { get; }
        public IOPath TablePath { get; }

        /// <summary>
        /// Construct the latest snapshot for given table.
        /// </summary>
        /// <param name="engine">Instance of <see cref="IEngine"/> to use.</param>
        /// <returns></returns>
        public async Task<TableSnapshot> BuildLatestSnapshot(IEngine engine) {
            return await GetSnapshotAtInit(engine);
        }

        /// <summary>
        /// Load the Snapshot for this Delta table at initialization.
        /// This method uses the `lastCheckpoint` file as a hint on where to start listing the transaction log directory.
        /// </summary>
        /// <param name="engine"></param>
        /// <returns></returns>
        private async Task<TableSnapshot> GetSnapshotAtInit(IEngine engine) {
            var checkpointer = new Checkpointer(LogPath);
            CheckpointMetaData? lastCheckpointOpt = await checkpointer.ReadLastCheckpointFile(engine);
            if(lastCheckpointOpt == null) {
                Debug.WriteLine($"{TablePath}: Last checkpoint file is missing or corrupted. Will search for the checkpoint files directly.");
            }

            LogSegment? logSegmentOpt = await GetLogSegmentFrom(engine, lastCheckpointOpt);

            if(logSegmentOpt == null)
                throw new TableNotFoundException(TablePath);

            return await GetCoordinatedCommitsAwareSnapshot(engine, logSegmentOpt, null);
        }



        /// <summary>
        /// This can be optimized by making snapshot hint optimization to work with coordinated commits.
        /// See https://github.com/delta-io/delta/issues/3437</a>.
        /// </summary>
        /// <param name="engine"></param>
        /// <param name="initialSegmentForNewSnapshot"></param>
        /// <param name="versionToLoadOpt"></param>
        /// <returns></returns>
        private async Task<TableSnapshot> GetCoordinatedCommitsAwareSnapshot(IEngine engine,
            LogSegment initialSegmentForNewSnapshot, long? versionToLoadOpt) {

            TableSnapshot newSnapshot = await CreateSnapshot(initialSegmentForNewSnapshot, engine);

            if(versionToLoadOpt != null && newSnapshot.Version == versionToLoadOpt) {
                return newSnapshot;
            }

            TableCommitCoordinatorClientHandler? newTableCommitCoordinatorClientHandlerOpt =
                newSnapshot.GetTableCommitCoordinatorClientHandlerOpt(engine);

            if(newTableCommitCoordinatorClientHandlerOpt != null) {
                // todo
            }

            return newSnapshot;
        }

        private async Task<TableSnapshot> CreateSnapshot(LogSegment initSegment, IEngine engine) {
            Debug.WriteLine($"{TablePath}: Loading version {initSegment.Version} starting from checkpoint version {initSegment.CheckpointVersion?.ToString() ?? "."}");

            LogReplay logReplay = await LogReplay.CreateAsync(LogPath, TablePath, initSegment.Version, engine, initSegment, latestSnapshotHint);

            DateTime startTime = DateTime.UtcNow;
            LogReplay.AssertLogFilesBelongToTable(LogPath, initSegment.AllFiles);

            var snapshot = new TableSnapshot(TablePath, initSegment, logReplay, logReplay.Protocol, logReplay.Metadata);
            Debug.WriteLine($"{TablePath}: Took {(DateTime.UtcNow - startTime).TotalMilliseconds}ms to construct the snapshot (loading protocol and metadata) for {initSegment.Version} {initSegment.CheckpointVersion}");

            var hint = new SnapshotHint(snapshot.Version, snapshot.Protocol, snapshot.Metadata);

            RegisterHint(hint);

            return snapshot;
        }

        /// <summary>
        /// Updates the current `latestSnapshotHint` with the `newHint` if and only if the newHint is newer
        /// (i.e.has a later table version).
        /// </summary>
        /// <param name="newHint"></param>
        private void RegisterHint(SnapshotHint newHint) {

            latestSnapshotHint = latestSnapshotHint == null
                ? newHint
                : newHint.Version > latestSnapshotHint.Version
                    ? newHint
                    : latestSnapshotHint;
        }

        /// <summary>
        /// Get the LogSegment that will help in computing the Snapshot of the table at DeltaLog initialization, or null if the directory was empty/missing.
        /// </summary>
        /// <param name="engine"></param>
        /// <param name="startingCheckpoint">A checkpoint that we can start our listing from</param>
        /// <returns></returns>
        private Task<LogSegment?> GetLogSegmentFrom(
            IEngine engine, CheckpointMetaData? startingCheckpoint) {
            return GetLogSegmentAtOrBeforeVersion(
                engine, startingCheckpoint?.Version, null, null);
        }

        /// <summary>
        /// Get a list of files that can be used to compute a Snapshot at or before version
        /// `versionToLoad`, If `versionToLoad` is not provided, will generate the list of files that are
        /// needed to load the latest version of the Delta table.This method also performs checks to
        /// ensure that the delta files are contiguous.
        /// </summary>
        /// <param name="engine"></param>
        /// <param name="startCheckpoint">
        /// A potential start version to perform the listing of the DeltaLog, typically that of a known checkpoint.
        /// If this version's not provided, we will start listing from version 0.
        /// </param>
        /// <param name="versionToLoad">
        /// A specific version we try to load, but we may only load a version before
        /// this version if this version of commit is un-backfilled.Typically used with time travel
        /// and the Delta streaming source.If not provided, we will try to load the latest version of
        /// the table.
        /// </param>
        /// <param name="tableCommitHandlerOpt"></param>
        /// <returns></returns>
        private async Task<LogSegment?> GetLogSegmentAtOrBeforeVersion(
            IEngine engine,
            long? startCheckpoint,
            long? versionToLoad,
            TableCommitCoordinatorClientHandler? tableCommitHandlerOpt) {

            // Only use startCheckpoint if it is <= versionToLoad
            long? startCheckpointToUse = versionToLoad != null && startCheckpoint != null && startCheckpoint <= versionToLoad
                ? startCheckpoint
                : null;

            // if we are loading a specific version and there is no usable starting checkpoint
            // try to load a checkpoint that is <= version to load
            if(startCheckpointToUse == null && versionToLoad != null) {
                long beforeVersion = versionToLoad.Value + 1;
                startCheckpointToUse = (await Checkpointer.FindLastCompleteCheckpointBefore(engine, LogPath, beforeVersion))?.Version;
            }

            long startVersion;
            if(startCheckpointToUse == null) {
                Debug.WriteLine($"{TablePath}: Starting checkpoint is missing. Listing from version as 0");

                startVersion = 0;

            } else {
                startVersion = startCheckpointToUse.Value;
            }

            DateTime startTime = DateTime.UtcNow;
            List<IOEntry>? newFiles = await ListDeltaAndCheckpointFiles(
                engine, startVersion, versionToLoad, tableCommitHandlerOpt);
            Debug.WriteLine($"{TablePath}: Took {(DateTime.UtcNow - startTime).TotalMilliseconds}ms to list the files after starting checkpoint");

            startTime = DateTime.UtcNow;
            LogSegment? logSegment = await GetLogSegmentAtOrBeforeVersion(engine, startCheckpointToUse, versionToLoad, newFiles, tableCommitHandlerOpt);
            Debug.WriteLine($"{TablePath}: Took {(DateTime.UtcNow - startTime).TotalMilliseconds}ms to construct a log segment");
            return logSegment;
        }

        protected async Task<LogSegment?> GetLogSegmentAtOrBeforeVersion(
            IEngine engine,
            long? startCheckpointOpt,
            long? versionToLoadOpt,
            List<IOEntry>? filesOpt,
            TableCommitCoordinatorClientHandler? tableCommitHandlerOpt) {
            List<IOEntry> newFiles;
            if(filesOpt == null) {
                // No files found even when listing from 0 => empty directory =>
                // table does not exist yet.
                if(startCheckpointOpt == null) {
                    return null;
                }

                // FIXME: We always write the commit and checkpoint files before updating
                //  _last_checkpoint. If the listing came up empty, then we either encountered a
                // list-after-put inconsistency in the underlying log store, or somebody corrupted the
                // table by deleting files. Either way, we can't safely continue.
                //
                // For now, we preserve existing behavior by returning Array.empty, which will trigger a
                // recursive call to [[getLogSegmentForVersion]] below (same as before the refactor).
                newFiles = new List<IOEntry>();

            } else {
                newFiles = filesOpt;
            }

            DebugX.WriteCollection("newFiles", newFiles.Select(f => f.Path.ToString()));

            if(newFiles.Count == 0 && startCheckpointOpt == null) {
                // We can't construct a snapshot because the directory contained no usable commit
                // files... but we can't return null either, because it was not truly null, so we throw an exception.
                throw new Exception($"No delta files found in the directory: {LogPath}");
            } else if(newFiles.Count == 0) {
                // The directory may be deleted and recreated and we may have stale state in our
                // DeltaLog singleton, so try listing from the first version
                return await GetLogSegmentAtOrBeforeVersion(
                    engine, null, versionToLoadOpt, tableCommitHandlerOpt);
            }

            var checkpoints = newFiles.Where(f => f.Path.IsCheckpointFile()).ToList();
            var deltas = newFiles.Where(f => !f.Path.IsCheckpointFile()).ToList();

            DebugX.WriteCollection("checkpoints", checkpoints.Select(f => f.Path.ToString()));
            DebugX.WriteCollection("deltas", deltas.Select(f => f.Path.ToString()));

            // Find the latest checkpoint in the listing that is not older than the versionToLoad
            CheckpointInstance maxCheckpoint = versionToLoadOpt == null
                ? CheckpointInstance.MaxValue
                : new CheckpointInstance(versionToLoadOpt.Value);
            Debug.WriteLine($"lastCheckpoint: {maxCheckpoint}");

            List<CheckpointInstance> checkpointFiles = checkpoints.Select(f => new CheckpointInstance(f.Path)).ToList();
            DebugX.WriteCollection("checkpointFiles", checkpointFiles.Select(f => f.ToString()));

            CheckpointInstance? newCheckpointOpt =
                Checkpointer.GetLatestCompleteCheckpointFromList(checkpointFiles, maxCheckpoint);
            Debug.WriteLine($"newCheckpointOpt: {newCheckpointOpt}");

            long newCheckpointVersion;
            if(newCheckpointOpt == null) {
                // If we do not have any checkpoint, pass new checkpoint version as -1 so that
                // first delta version can be 0.
                newCheckpointVersion = -1;
            } else {
                newCheckpointVersion = newCheckpointOpt.Version;
            }
            Debug.WriteLine("newCheckpointVersion: " + newCheckpointVersion);

            // TODO: we can calculate deltasAfterCheckpoint and deltaVersions more efficiently
            // If there is a new checkpoint, start new lineage there. If `newCheckpointVersion` is -1,
            // it will list all existing delta files.
            List<IOEntry> deltasAfterCheckpoint = deltas
                .Where(e => e.Path.GetDeltaVersion() > newCheckpointVersion)
                .ToList();
            DebugX.WriteCollection("deltasAfterCheckpoint", deltasAfterCheckpoint.Select(f => f.ToString()));

            List<long> deltaVersionsAfterCheckpoint = deltasAfterCheckpoint
                .Select(deltasAfterCheckpoint => deltasAfterCheckpoint.Path.GetDeltaVersion())
                .ToList();
            DebugX.WriteCollection("deltaVersions", deltaVersionsAfterCheckpoint.Select(f => f.ToString()));

            long newVersion = deltaVersionsAfterCheckpoint.Any() ? deltaVersionsAfterCheckpoint.Last() : newCheckpointOpt!.Version;

            // There should be a delta file present for the newVersion that we are loading
            // (Even if `deltasAfterCheckpoint` is empty, `deltas` should not be)
            if(!deltas.Any() || deltas.Last().Path.GetDeltaVersion() < newVersion) {
                throw new InvalidTableException(TablePath, $"Missing delta file for version {newVersion}");
            }

            if(versionToLoadOpt != null && newVersion > versionToLoadOpt) {
                throw new KernelException($"{TablePath}: Cannot load table version {versionToLoadOpt} as it does not exist. The latest available version is {newVersion}.");
            }

            // We may just be getting a checkpoint file after the filtering
            if(deltaVersionsAfterCheckpoint.Any()) {
                // If we have deltas after the checkpoint, the first file should be 1 greater than our
                // last checkpoint version. If no checkpoint is present, this means the first delta file
                // should be version 0.

                if(deltaVersionsAfterCheckpoint.First() != newCheckpointVersion + 1) {
                    throw new InvalidTableException(
                        TablePath,
                        $"Unable to reconstruct table state: missing log file for version {newCheckpointVersion + 1}");
                }
                Debug.WriteLine($"Verified delta files are contiguous from version {newCheckpointVersion + 1} to {newVersion}");

                VerifyDeltaVersions(deltaVersionsAfterCheckpoint, newCheckpointVersion + 1, newVersion, TablePath);
            }

            DateTimeOffset lastCommitTimestamp = deltas.Last().LastModificationTime!.Value;

            List<IOEntry> newCheckpointFiles;
            if(newCheckpointOpt == null) {
                newCheckpointFiles = new List<IOEntry>();
            } else {
                var newCheckpointPaths = new HashSet<IOPath>(newCheckpointOpt.GetCorrespondingFiles(LogPath));
                newCheckpointFiles = checkpoints
                    .Where(f => newCheckpointPaths.Contains(f.Path))
                    .ToList();

                if(newCheckpointFiles.Count != newCheckpointPaths.Count) {
                    var msg = new StringBuilder();
                    msg.AppendLine("Seems like the checkpoint is corrupted. Failed in getting the file information for:");
                    foreach(IOPath path in newCheckpointPaths) {
                        msg.Append(" - ");
                        msg.AppendLine(path);
                    }
                    msg.AppendLine("among");
                    foreach(IOEntry entry in checkpoints) {
                        msg.Append(" - ");
                        msg.AppendLine(entry.Path);
                    }
                    throw new Exception(msg.ToString());
                }
            }

            return new LogSegment(LogPath, newVersion, deltasAfterCheckpoint, newCheckpointFiles, newCheckpointOpt?.Version, lastCommitTimestamp);
        }


        /// <summary>
        /// - Verify the versions are contiguous.
        /// - Verify the versions start with `expectedStartVersion` if it's specified.
        /// - Verify the versions end with `expectedEndVersion` if it's specified.
        /// </summary>
        /// <param name="versions"></param>
        /// <param name="expectedStartVersion"></param>
        /// <param name="expectedEndVersion"></param>
        /// <param name="tablePath"></param>
        /// <exception cref="InvalidTableException"></exception>
        public static void VerifyDeltaVersions(
            List<long> versions,
            long? expectedStartVersion,
            long? expectedEndVersion,
            IOPath tablePath) {
            if(versions.Any()) {
                // verify the versions are contiguous
                long v = 0;
                foreach(long version in versions) {
                    if(version != v) {
                        throw new InvalidTableException(
                            tablePath,
                            $"Missing delta files: versions are not continuous: ({v})");
                    }
                    v += 1;
                }
            }
            if(expectedStartVersion != null) {
                if(!versions.Any() && versions.First() != expectedStartVersion) {
                    throw new InvalidTableException(
                        tablePath,
                        $"Did not get the first delta file version {expectedStartVersion} to compute Snapshot");
                }
            };

            if(expectedEndVersion != null) {
                if(!versions.Any() && versions.Last() != expectedEndVersion) {
                    throw new InvalidTableException(
                    tablePath,
                    $"Did not get the last delta file version {expectedEndVersion} to compute Snapshot");
                }
            }
        }

        protected async Task<List<IOEntry>?> ListDeltaAndCheckpointFiles(
            IEngine engine,
            long startVersion,
            long? versionToLoad,
            TableCommitCoordinatorClientHandler? tableCommitHandlerOpt) {
            if(versionToLoad != null) {
                if(versionToLoad < startVersion) {
                    throw new ArgumentException($"versionToLoad={versionToLoad} is less than startVersion={startVersion}");
                }
            }
            Debug.WriteLine(
                "startVersion: {0}, versionToLoad: {1}, coordinated commits enabled: {2}",
                startVersion,
                versionToLoad,
                tableCommitHandlerOpt != null);

            // Fetching the unbackfilled commits before doing the log directory listing to avoid a gap
            // in delta versions if some delta files are backfilled after the log directory listing but
            // before the unbackfilled commits listing
            IReadOnlyCollection<Commit> unbackfilledCommits =
                getUnbackfilledCommits(tableCommitHandlerOpt, startVersion, versionToLoad);

            long maxDeltaVersionSeen = startVersion - 1;
            IReadOnlyCollection<IOEntry> listing = await engine.ListFrom(LogPath, startVersion.GetVersionListingPrefix());
            var resultFromFsListingOpt = new List<IOEntry>();
            foreach(IOEntry entry in listing) {
                // Pick up all checkpoint and delta files
                if(!isDeltaCommitOrCheckpointFile(entry.Path)) {
                    continue;
                }

                // Checkpoint files of 0 size are invalid but may be ignored silently when read,
                // hence we drop them so that we never pick up such checkpoints.
                if(entry.Path.IsCheckpointFile() && ((entry.Size ?? 0) == 0)) {
                    continue;
                }

                // Take files until the version we want to load
                bool versionWithinRange = versionToLoad == null
                    ? true
                    : entry.Path.GetFileVersion() <= versionToLoad;

                if(!versionWithinRange) {
                    throw new NotImplementedException();
                }

                // Ideally listFrom should return lexicographically sorted
                // files and so maxDeltaVersionSeen should be equal to fileVersion.
                // But we are being defensive here and taking max of all the
                // fileVersions seen.
                if(entry.Path.IsCommitFile()) {
                    maxDeltaVersionSeen = Math.Max(maxDeltaVersionSeen, entry.Path.GetDeltaVersion());
                }
                resultFromFsListingOpt.Add(entry);
            }

            if(tableCommitHandlerOpt == null)
                return resultFromFsListingOpt;

            throw new NotImplementedException();
        }

        /// <summary>
        /// Returns true if the given file name is delta log files. Delta log files can be delta commit
        /// file (e.g., 000000000.json), or checkpoint file. (e.g., 000000001.checkpoint.00001.00003.parquet)
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        private bool isDeltaCommitOrCheckpointFile(IOPath path) {
            return path.IsCheckpointFile() || path.IsCommitFile();
        }

        private IReadOnlyCollection<Commit> getUnbackfilledCommits(
            TableCommitCoordinatorClientHandler? tableCommitHandlerOpt,
            long startVersion,
            long? versionToLoad) {

            if(tableCommitHandlerOpt == null) {
                return new List<Commit>();
            }

            throw new NotImplementedException();
        }
    }
}