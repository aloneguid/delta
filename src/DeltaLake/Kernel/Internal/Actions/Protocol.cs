using System.Text.Json.Serialization;

namespace DeltaLake.Kernel.Internal.Actions {
    public class Protocol : Action {

        public Protocol() : base(ActionType.Protocol) {
        }

        /// <summary>
        /// The minimum version of the Delta read protocol that a client must implement in order to correctly read this table
        /// </summary>
        [JsonPropertyName("minReaderVersion")]
        public int? MinReaderVersion { get; set; }

        /// <summary>
        /// The minimum version of the Delta write protocol that a client must implement in order to correctly write this table
        /// </summary>
        [JsonPropertyName("minWriterVersion")]
        public int? MinWriterVersion { get; set; }

        /// <summary>
        /// A collection of features that a client must implement in order to correctly read this table (exist only when minReaderVersion is set to 3)
        /// </summary>
        [JsonPropertyName("readerFeatures")]
        public string[]? ReaderFeatures { get; set; }

        /// <summary>
        /// A collection of features that a client must implement in order to correctly write this table (exist only when minWriterVersion is set to 7)
        /// </summary>
        [JsonPropertyName("writerFeatures")]
        public string[]? WriterFeatures { get; set; }
    }
}
