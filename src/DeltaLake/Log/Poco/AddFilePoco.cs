using System.Text.Json.Serialization;

namespace DeltaLake.Log.Poco {
    class AddFilePoco {
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

        /// <summary>
        /// Required.
        /// The time this logical file was created, as milliseconds since the epoch.
        /// </summary>
        [JsonPropertyName("modificationTime")]
        public long? ModificationTime { get; set; }

        /// <summary>
        /// Required.
        /// When false the logical file must already be present in the table or the records in the added file must
        /// be contained in one or more remove actions in the same version.
        /// </summary>
        [JsonPropertyName("dataChange")]
        public bool? DataChange { get; set; }

        // todo: stats (optional)

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

        /// <summary>
        /// The name of the clustering implementation.
        /// </summary>
        [JsonPropertyName("clusteringProvider")]
        public string? ClusteringProvider { get; set; }
    }
}
