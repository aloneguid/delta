using System.Text.Json.Serialization;

namespace DeltaLake.Log.Actions {
    internal class ChangeMetadataPoco {
        /// <summary>
        /// Unique identifier for this table
        /// </summary>
        [JsonPropertyName("id")]
        public string? Id { get; set; }

        /// <summary>
        /// User-provided identifier for this table
        /// </summary>
        [JsonPropertyName("name")]
        public string? Name { get; set; }

        /// <summary>
        /// User-provided description for this table
        /// </summary>
        [JsonPropertyName("description")]
        public string? Description { get; set; }

        /// <summary>
        /// Specification of the encoding for the files stored in the table
        /// </summary>
        [JsonPropertyName("format")]
        public MetadataFormatPoco? Format { get; set; }

        // todo: schemaString

        /// <summary>
        /// An array containing the names of columns by which the data should be partitioned.
        /// </summary>
        [JsonPropertyName("partitionColumns")]
        public string[]? PartitionColumns { get; set; }

        /// <summary>
        /// The time when this metadata action is created, in milliseconds since the Unix epoch
        /// </summary>
        [JsonPropertyName("createdTime")]
        public long? CreatedTime { get; set; }

        /// <summary>
        /// A map containing configuration options for the metadata action
        /// </summary>
        [JsonPropertyName("configuration")]
        public Dictionary<string, string>? Configuration { get; set; }
    }
}
