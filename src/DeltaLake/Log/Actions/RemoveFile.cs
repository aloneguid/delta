using System.Text.Json.Serialization;

namespace DeltaLake.Log.Actions {
    public class RemoveFile : FileBase {

        public RemoveFile() : base(ActionType.RemoveFile) {
        }

        /// <summary>
        /// The time the deletion occurred, represented as milliseconds since the epoch.
        /// </summary>
        [JsonPropertyName("deletionTimestamp")]
        public long? DeletionTimestamp { get; set; }

        /// <summary>
        /// When false the logical file must already be present in the table or the records in the added file must
        /// be contained in one or more remove actions in the same version.
        /// </summary>
        [JsonPropertyName("dataChange")]
        public bool? DataChange { get; set; }

        /// <summary>
        /// When true the fields partitionValues, size, and tags are present.
        /// </summary>
        [JsonPropertyName("extendedFileMetadata")]
        public bool? ExtendedFileMetadata { get; set; }

        /// <summary>
        /// A map from partition column to value for this logical file. See also Partition Value Serialization.
        /// </summary>
        [JsonPropertyName("partitionValues")]
        public Dictionary<string, string>? PartitionValues { get; set; }

        // todo: stats

        /// <summary>
        /// Map containing metadata about this logical file.
        /// </summary>
        [JsonPropertyName("tags")]
        public Dictionary<string, string>? Tags { get; set; }

        // todo: deletionVector

        /// <summary>
        /// Default generated Row ID of the first row in the file.
        /// The default generated Row IDs of the other rows in the file can be reconstructed by adding the physical index
        /// of the row within the file to the base Row ID. See also Row IDs.
        /// </summary>
        [JsonPropertyName("baseRowId")]
        public long? BaseRowId { get; set; }

        /// <summary>
        /// First commit version in which an add action with the same path was committed to the table.
        /// </summary>
        [JsonPropertyName("defaultRowCommitVersion")]
        public long? DefaultRowCommitVersion { get; set; }

        [JsonIgnore]
        public override long? Timestamp => DeletionTimestamp;

        public override string ToString() => $"remove {Path}";
    }
}
