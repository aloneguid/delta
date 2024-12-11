using System.Text.Json.Serialization;

namespace DeltaLake.Kernel.Internal.Actions {

    public class MetadataFormat {
        /// <summary>
        /// Name of the encoding for files in this table
        /// </summary>
        public string Provider { get; set; } = "parquet";

        /// <summary>
        /// A map containing configuration options for the format
        /// </summary>
        public Dictionary<string, string> Options { get; set; } = new Dictionary<string, string>();

        public override string ToString() => Provider;
    }

    public class Metadata : Action {

        public Metadata() : base(ActionType.Metadata) {
        }

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
        public MetadataFormat? Format { get; set; }

        /// <summary>
        /// Schema of the table
        /// </summary>
        [JsonPropertyName("schemaString")]
        public string? SchemaString { get; set; }

        /// <summary>
        /// An array containing the names of columns by which the data should be partitioned.
        /// </summary>
        [JsonPropertyName("partitionColumns")]
        public string[]? PartitionColumns { get; set; }

        /// <summary>
        /// The time when this metadata action is created, in milliseconds since the Unix epoch
        /// </summary>
        [JsonPropertyName("createdTime")]
        public long? CreatedTimeUnixMilliseconds { get; set; }

        [JsonIgnore]
        public DateTime CreatedTimeDateTime => CreatedTimeUnixMilliseconds!.Value.FromUnixTimeMilliseconds();

        /// <summary>
        /// A map containing configuration options for the metadata action
        /// </summary>
        [JsonPropertyName("configuration")]
        public Dictionary<string, string>? Configuration { get; set; }
    }
}
