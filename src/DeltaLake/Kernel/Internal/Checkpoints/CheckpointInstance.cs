using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics.X86;
using System.Text;
using System.Threading.Tasks;
using Parquet.Schema;
using Stowage;

namespace DeltaLake.Kernel.Internal.Checkpoints {

    public enum CheckpointFormat {

        // Note that the order of these enum values is important for comparison of checkpoint
        // instances (we prefer V2 > MULTI_PART > CLASSIC).

        Classic = 0,
        Multipart = 1,
        V2 = 2
    }

    /// <summary>
    /// Metadata about Delta checkpoint.
    /// </summary>
    public class CheckpointInstance : IEquatable<CheckpointInstance>, IComparer<CheckpointInstance> {

        /// <summary>
        /// Placeholder to identify the version that is always the latest on timeline
        /// </summary>
        public static CheckpointInstance MaxValue { get; } = new CheckpointInstance(long.MaxValue);

        /// <summary>
        /// Indicates that the checkpoint (may) contain SidecarFile actions. For compatibility,
        /// V2 checkpoints can be named with classic-style names, so any checkpoint other than a
        /// multipart checkpoint may contain SidecarFile actions.
        /// </summary>
        /// <param name="format"></param>
        /// <returns></returns>
        public static bool UseSidecars(CheckpointFormat format) {
            return format == CheckpointFormat.Classic || format == CheckpointFormat.Multipart;
        }

        public CheckpointInstance(long version) : this(version, null) { }

        public CheckpointInstance(long version, int? numParts) {
            Version = version;
            NumParts = numParts;
            Path = null;
            if((numParts ?? 0) == 0) {
                Format = CheckpointFormat.Classic;
            } else {
                Format = CheckpointFormat.Multipart;
            }
        }

        public CheckpointInstance(IOPath path) {
            if(!path.IsCheckpointFile())
                throw new ArgumentException("not a valid checkpoint file name");

            string[] pathParts = path.Name.Split('.');

            if(pathParts.Length == 3 && pathParts[2] == "parquet") {
                // Classic checkpoint 00000000000000000010.checkpoint.parquet
                Version = long.Parse(pathParts[0]);
                NumParts = null;
                Format = CheckpointFormat.Classic;
                Path = null;
            } else if(pathParts.Length == 5 && pathParts[4] == "parquet") {
                // Multi-part checkpoint 00000000000000000010.checkpoint.0000000001.0000000003.parquet
                Version = long.Parse(pathParts[0]);
                NumParts = int.Parse(pathParts[3]);
                Format = CheckpointFormat.Multipart;
                Path = null;
            } else if(pathParts.Length == 4
                && (pathParts[3] == "parquet" || pathParts[3] == "json")) {
                // V2 checkpoint 00000000000000000010.checkpoint.UUID.(parquet|json)
                Version = long.Parse(pathParts[0]);
                NumParts = null;
                Format = CheckpointFormat.V2;
                Path = path;
            } else {
                throw new Exception("Unrecognized checkpoint path format: " + path.Name);
            }
        }

        public long Version { get; }

        public int? NumParts { get; }

        /// <summary>
        /// Guaranteed to be present for V2 checkpoints.
        /// </summary>
        public IOPath? Path { get; }

        public CheckpointFormat Format { get; }

        public bool IsNotLaterThan(CheckpointInstance other) {
            if(other == MaxValue) {
                return true;
            }
            return Version <= other.Version;
        }

        public List<IOPath> GetCorrespondingFiles(IOPath path) {
            if(this == MaxValue) {
                throw new ArgumentException("Can't get files for CheckpointVersion.MaxValue.");
            }

            // This is safe because the only way to construct a V2 CheckpointInstance is with the path.
            if(Format == CheckpointFormat.V2) {
                return new List<IOPath> { Path! };
            }

            return NumParts == null
                ? new List<IOPath> { path.CheckpointFileSingular(Version) }
                : path.CheckpointFileWithParts(Version, NumParts.Value);
        }


        public override string ToString() => $"version={Version}, numParts={NumParts}, format={Format}, filePath={Path}";

        public bool Equals(CheckpointInstance? other) {
            if(ReferenceEquals(this, other)) {
                return true;
            }

            if(other == null || other.GetType() != GetType()) {
                return false;
            }

            return Compare(this, other) == 0;
        }

        /// <summary>
        /// For V2 checkpoints, the filepath is included in the hash of the instance (as we consider
        /// different UUID checkpoints to be different checkpoint instances. Otherwise, ignore
        /// the filepath (which is empty) when hashing.
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode() => HashCode.Combine(Version, NumParts, Path, Format);

        /// <summary>
        /// Comparison rules:
        /// 1. A CheckpointInstance with higher version is greater than the one with lower version.
        /// 2. A CheckpointInstance for a V2 checkpoint is greater than a classic checkpoint (to filter avoid selecting the compatibility file) or a multipart checkpoint.
        /// 3. For CheckpointInstances with same version, a Multi-part checkpoint is greater than a Single part checkpoint.
        /// 4. For Multi-part CheckpointInstance corresponding to same version, the one with more parts is greater than the one with fewer parts.
        /// 5. For V2 checkpoints, use the file path to break ties.
        /// </summary>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentException">When checkpoint format (enum) is unknown</exception>
        public int Compare(CheckpointInstance? x, CheckpointInstance? y) {
            if(x == null && y == null) {
                return 0;
            }

            if(x == null)
                return -1;

            if(y == null)
                return 1;

            // Compare versions.
            if(Version != y.Version) {
                return Version.CompareTo(y.Version);
            }

            // Compare formats.
            if(x.Format != y.Format) {
                return x.Format.CompareTo(y.Format);
            }

            // Use format-specific tiebreakers if versions and formats are the same.
            switch(x.Format) {
                case CheckpointFormat.Classic:
                    return 0; // No way to break ties if both are classic checkpoints.
                case CheckpointFormat.Multipart:
                    return (NumParts ?? 1).CompareTo(y.NumParts ?? 1);
                case CheckpointFormat.V2:
                    return Path!.Name.CompareTo(y.Path!.Name);
                default:
                    throw new ArgumentException("Unexpected format: " + x.Format);
            }
        }

    }
}
