using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace DeltaLake.Kernel.Internal.Actions {
    public abstract class FileBase : Action {
        public FileBase(ActionType action) : base(action) {
        }

        /// <summary>
        /// Required.
        /// A relative path to a data file from the root of the table or an absolute path to a file that should be added to the table.
        /// The path is a URI as specified by RFC 2396 URI Generic Syntax, which needs to be decoded to get the data file path.
        /// </summary>
        [JsonPropertyName("path")]
        public string? Path { get; set; }

        /// <summary>
        /// Required.
        /// A map from partition column to value for this logical file. See also Partition Value Serialization.
        /// </summary>
        [JsonPropertyName("partitionValues")]
        public Dictionary<string, string>? PartitionValues { get; set; }

        /// <summary>
        /// Required.
        /// The size of this data file in bytes.
        /// </summary>
        [JsonPropertyName("size")]
        public long? Size { get; set; }

        [JsonIgnore]
        public abstract long? Timestamp { get; }

        /// <summary>
        /// Either null (or absent in JSON) when no DV is associated with this data file, or a struct (described below)
        /// that contains necessary information about the DV that is part of this logical file.
        /// </summary>
        [JsonPropertyName("deletionVector")]
        public DeletionVector? DeletionVector { get; set; }

        [JsonPropertyName("stats")]
        public string? Stats { get; set; }
    }
}
