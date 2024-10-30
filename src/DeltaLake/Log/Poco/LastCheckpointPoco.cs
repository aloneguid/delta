using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace DeltaLake.Log.Poco {

    /// <summary>
    /// A checkpoint contains the complete replay of all actions, up to and including the checkpointed table version,
    /// with invalid actions removed. Invalid actions are those that have been canceled out by subsequent ones
    /// (for example removing a file that has been added).
    /// See https://github.com/delta-io/delta/blob/master/PROTOCOL.md#checkpoints
    /// last checkpoint file schema: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#last-checkpoint-file-schema
    /// </summary>
    class LastCheckpointPoco {
        [JsonPropertyName("version")]
        public long Version { get; set; }

        [JsonPropertyName("size")]
        public long Size { get; set; }

        // todo: parts

        [JsonPropertyName("sizeInBytes")]
        public long? SizeInBytes { get; set; }

        [JsonPropertyName("numOfAddFiles")]
        public long? NumOfAddFiles { get; set; }

        // todo: checkpointSchema

        [JsonPropertyName("checksum")]
        public string? Checksum { get; set; }
    }
}
