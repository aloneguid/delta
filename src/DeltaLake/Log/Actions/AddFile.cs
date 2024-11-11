using System.Text.Json.Serialization;

namespace DeltaLake.Log.Actions {
    public class AddFile : FileBase {

        public AddFile() : base(ActionType.AddFile) { 
        }

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

        /// <summary>
        /// Either null (or absent in JSON) when no DV is associated with this data file, or a struct (described below)
        /// that contains necessary information about the DV that is part of this logical file.
        /// </summary>
        [JsonPropertyName("deletionVector")]
        public DeletionVector? DeletionVector { get; set; }

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

        [JsonIgnore]
        public override long? Timestamp => ModificationTime;

        public override void Validate() {
            if(Path == null) throw new ArgumentNullException(nameof(Path));
        }

        public override string ToString() => $"add {Path}";
    }
}
