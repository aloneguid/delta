using System;
using System.Diagnostics;
using System.Text.RegularExpressions;
using Stowage;

namespace DeltaLake.Kernel {
    static class Extensions {

        private static readonly Regex CheckpointFilePattern = new Regex("(\\d+)\\.checkpoint((\\.\\d+\\.\\d+)?\\.parquet|\\.[^.]+\\.(json|parquet))");

        private static readonly Regex ClassicCheckpointFilePattern = new Regex("\\d+\\.checkpoint\\.parquet");

        private static readonly Regex V2CheckpointFilePattern = new Regex("(\\d+)\\.checkpoint\\.[^.]+\\.(json|parquet)");

        private static readonly Regex MultiPartCheckpointFilePattern = new Regex("(\\d+)\\.checkpoint\\.\\d+\\.\\d+\\.parquet");


        private static readonly Regex DeltaFilePattern = new Regex("\\d+\\.json");

        // Example: 00000000000000000001.dc0f9f58-a1a0-46fd-971a-bd8b2e9dbb81.json
        private static readonly Regex UUIDDelgaFileRegex = new Regex("(\\d+)\\.([^\\.]+)\\.json");

        public static string GetVersionListingPrefix(this long version) => string.Format("{0:D20}", version);

        public static bool IsCheckpointFile(this IOPath entry) => CheckpointFilePattern.IsMatch(entry.Name);

        public static bool IsClassicCheckpointFile(this IOPath entry) => ClassicCheckpointFilePattern.IsMatch(entry.Name);

        public static bool IsMultiPartCheckpointFile(this IOPath entry) => MultiPartCheckpointFilePattern.IsMatch(entry.Name);

        public static bool IsV2CheckpointFile(this IOPath entry) => V2CheckpointFilePattern.IsMatch(entry.Name);

        public static bool IsCommitFile(this IOPath entry) => DeltaFilePattern.IsMatch(entry.Name) || UUIDDelgaFileRegex.IsMatch(entry.Name);

        public static long GetCheckpointVersion(this IOPath path) {
            string vs = path.Name;
            int idx = vs.IndexOf('.');
            if(idx != -1) {
                vs = vs.Substring(0, idx);
            }

            long.TryParse(vs, out long v);
            return v;
        }

        public static long GetDeltaVersion(this IOPath path) => GetCheckpointVersion(path);

        /// <summary>
        /// Get the version of the checkpoint, checksum or delta file. Throws an error if an unexpected
        /// file type is seen.These unexpected files should be filtered out to ensure forward
        /// compatibility in cases where new file types are added, but without an explicit protocol
        /// upgrade.
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        /// <exception cref="IllegalArgumentException"></exception>
        public static long GetFileVersion(this IOPath path) {
            if(IsCheckpointFile(path)) {
                return GetCheckpointVersion(path);
            } else if(IsCommitFile(path)) {
                return GetDeltaVersion(path);
                // } else if (isChecksumFile(path)) {
                //    checksumVersion(path);
            } else {
                throw new ArgumentException($"Unexpected file type found in transaction log: {path}");
            }
        }

        /// <summary>
        /// Returns the path for a singular checkpoint up to the given version.
        /// In a future protocol version this path will stop being written.
        /// </summary>
        /// <param name="path"></param>
        /// <param name="version"></param>
        /// <returns></returns>
        public static IOPath CheckpointFileSingular(this IOPath path, long version) {
            return path.Combine($"{version:D20}.checkpoint.parquet");
        }

        /// <summary>
        /// Returns the paths for all parts of the checkpoint up to the given version.
        /// In a future protocol version we will write this path instead of checkpointFileSingular.
        /// Example of the format: 00000000000000004915.checkpoint.0000000020.0000000060.parquet is
        /// checkpoint part 20 out of 60 for the snapshot at version 4915. Zero padding is for
        /// lexicographic sorting.
        /// </summary>
        /// <param name="path"></param>
        /// <param name="version"></param>
        /// <param name="numParts"></param>
        /// <returns></returns>
        public static List<IOPath> CheckpointFileWithParts(this IOPath path, long version, int numParts) {
            var result = new List<IOPath>();
            for(int i = 1; i < numParts + 1; i++) {
                result.Add($"{version:D20}.checkpoint.{i:D10}.{numParts:D10}.parquet");
            }
            return result;
        }

    }

    static class DebugX {
        [Conditional("DEBUG")]
        public static void WriteCollection(string header, IEnumerable<string?>? collection) {
            Console.WriteLine(header);
            if(collection == null) {
                Console.WriteLine("  null");
                return;
            }
            foreach(string? item in collection) {
                Console.Write("  ");
                Console.WriteLine(item == null ? "null" : item);
            }
        }
    }
}
