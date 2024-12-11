using System.Runtime.InteropServices;
using System;
using System.Text.Json.Serialization;
using DeltaLake.Kernel.Engine;
using DeltaLake.Log.Actions;
using Stowage;
using System.Diagnostics;

namespace DeltaLake.Kernel.Internal.Checkpoints {

    public class CheckpointMetaData {

        [JsonPropertyName("version")]
        public long? Version { get; set; }

        [JsonPropertyName("size")]
        public long? Size { get; set; }

        [JsonPropertyName("parts")]
        public long? Parts { get; set; }
    }

    public class Checkpointer {

        /// <summary>
        /// The name of the last checkpoint file.
        /// </summary>
        public const string LastCheckpointFileName = "_last_checkpoint";

        public Checkpointer(IOPath logPath) {
            LastCheckpointFilePath = logPath.Combine(LastCheckpointFileName);
        }
        public IOPath LastCheckpointFilePath { get; }

        /// <summary>
        /// Returns information about the most recent checkpoint.
        /// </summary>
        /// <param name="engine"></param>
        /// <returns></returns>
        public async Task<CheckpointMetaData?> ReadLastCheckpointFile(IEngine engine) {
            return await LoadMetadataFromFile(engine, 0 /* tries */);
        }


        /// <summary>
        /// Loads the checkpoint metadata from the _last_checkpoint file.
        /// </summary>
        /// <param name="engine"></param>
        /// <param name="tries">Number of times already tried to load the metadata before this call.</param>
        /// <returns></returns>
        private async Task<CheckpointMetaData?> LoadMetadataFromFile(IEngine engine, int tries) {
            if(tries >= 3) {
                // We have tried 3 times and failed. Assume the checkpoint metadata file is corrupt.
                //logger.warn(
                //    "Failed to load checkpoint metadata from file {} after 3 attempts.",
                //    lastCheckpointFilePath);
                //return Optional.empty();
                return null;
            }
            try {
                // Use arbitrary values for size and mod time as they are not available.
                // We could list and find the values, but it is an unnecessary FS call.
                var lastCheckpointFile = new IOEntry(LastCheckpointFilePath);

                CheckpointMetaData? result = await engine.FileStorage.ReadAsJson<CheckpointMetaData>(LastCheckpointFilePath);
                if(result != null)
                    return result;

                // Checkpoint has no data. This is a valid case on some file systems where the contents are not visible until the file stream is closed.
                // Sleep for one second and retry.

                await Task.Delay(1000);

                bool isPresent = await engine.FileStorage.Exists(LastCheckpointFilePath);

                return await LoadMetadataFromFile(engine, tries + 1);
            } catch {
                await Task.Delay(1000);

                return await LoadMetadataFromFile(engine, tries + 1);
            }
        }

        /** Find the last complete checkpoint before (strictly less than) a given version. */
        public static async Task<CheckpointInstance?> FindLastCompleteCheckpointBefore(
            IEngine engine, IOPath tableLogPath, long version) {
            return (await FindLastCompleteCheckpointBeforeHelper(engine, tableLogPath, version)).Item1;
        }

        /// <summary>
        /// Helper method for `findLastCompleteCheckpointBefore` which also return the number of files
        /// searched. This helps in testing.
        /// </summary>
        /// <param name="engine"></param>
        /// <param name="tableLogPath"></param>
        /// <param name="version"></param>
        /// <returns></returns>
        protected static Task<Tuple<CheckpointInstance?, long>> FindLastCompleteCheckpointBeforeHelper(IEngine engine,
            IOPath tableLogPath, long version) {
            var upperBoundCheckpoint = new CheckpointInstance(version);
            Debug.WriteLine("Try to find the last complete checkpoint before version {0}", version);

            // This is a just a tracker for testing purposes
            long numberOfFilesSearched = 0;
            long currentVersion = version;

            // Some cloud storage APIs make a calls to fetch 1000 at a time.
            // To make use of that observation and to avoid making more listing calls than
            // necessary, list 1000 at a time (backwards from the given version). Search
            // within that list if a checkpoint is found. If found stop, otherwise list the previous
            // 1000 entries. Repeat until a checkpoint is found or there are no more delta commits.
            throw new NotImplementedException();
        }

        /// <summary>
        /// Given a list of checkpoint files, pick the latest complete checkpoint instance which is not
        /// later than `notLaterThan`.
        /// </summary>
        /// <param name="instances"></param>
        /// <param name="notLaterThan"></param>
        /// <returns></returns>
        public static CheckpointInstance? GetLatestCompleteCheckpointFromList(
            List<CheckpointInstance> instances, CheckpointInstance notLaterThan) {

            List<CheckpointInstance> completeCheckpoints = instances
                .Where(c => c.IsNotLaterThan(notLaterThan))
                .GroupBy(c => c)
                .Where(g => g.Key.NumParts == null ? g.Count() == 1 : g.Count() == g.Key.NumParts)
                .Select(g => g.Key)
                .ToList();

            return completeCheckpoints.Any() ? completeCheckpoints.Max() : null;
        }

    }
}
