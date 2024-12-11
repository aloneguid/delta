using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DeltaLake.Kernel.Exceptions;
using Stowage;

namespace DeltaLake.Kernel.Internal.Replay {

    public enum DeltaLogFileType {
        Commit,
        CheckpointClassic,
        MultipartCheckpoint,
        V2CheckpointManifest,
        Sidecar
    }


    /// <summary>
    /// Internal wrapper class holding information needed to perform log replay. Represents either a
    /// Delta commit file, classic checkpoint, a multipart checkpoint, a V2 checkpoint, or a sidecar
    /// checkpoint.
    /// </summary>
    public class DeltaLogFile {

        private DeltaLogFile(IOEntry file, DeltaLogFileType logType, long version) {
            File = file;
            LogType = logType;
            Version = version;
        }

        public IOEntry File { get; }
        public DeltaLogFileType LogType { get; }
        public long Version { get; }

        public static DeltaLogFile FromCommitOrCheckpoint(IOEntry file) {
            DeltaLogFileType logType;
            long version = -1;
            if(file.Path.IsCommitFile()) {
                logType = DeltaLogFileType.Commit;
                version = file.Path.GetDeltaVersion();
            } else if(file.Path.IsClassicCheckpointFile()) {
                logType = DeltaLogFileType.CheckpointClassic;
                version = file.Path.GetCheckpointVersion();
            } else if(file.Path.IsMultiPartCheckpointFile()) {
                logType = DeltaLogFileType.MultipartCheckpoint;
                version = file.Path.GetCheckpointVersion();
            } else if(file.Path.IsV2CheckpointFile()) {
                logType = DeltaLogFileType.V2CheckpointManifest;
                version = file.Path.GetCheckpointVersion();
            } else {
                throw new KernelException("File is not a commit or checkpoint file: " + file);
            }
            return new DeltaLogFile(file, logType, version);
        }
    }
}
