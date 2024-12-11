using System.Text.Json.Serialization;

namespace DeltaLake.Kernel.Internal.Actions {
    public class DeletionVector {
        /// <summary>
        /// A single character to indicate how to access the DV. Legal options are: ['u', 'i', 'p'].
        /// </summary>
        [JsonPropertyName("storageType")]
        public string? StorageType { get; set; }

        /// <summary>
        /// Three format options are currently proposed:
        /// - If storageType = 'u' then <random prefix - optional><base85 encoded uuid>: The deletion vector is stored in a file with a path relative to the data directory of this Delta table, and the file name can be reconstructed from the UUID. See Derived Fields for how to reconstruct the file name. The random prefix is recovered as the extra characters before the (20 characters fixed length) uuid.
        /// - If storageType = 'i' then <base85 encoded bytes>: The deletion vector is stored inline in the log. The format used is the RoaringBitmapArray format also used when the DV is stored on disk and described in Deletion Vector Format.
        /// - If storageType = 'p' then <absolute path>: The DV is stored in a file with an absolute path given by this path, which has the same format as the path field in the add/remove actions.
        /// </summary>
        [JsonPropertyName("pathOrInlineDb")]
        public string? PathOrInlineDv { get; set; }

        /// <summary>
        /// Start of the data for this DV in number of bytes from the beginning of the file it is stored in. Always None (absent in JSON) when storageType = 'i'.
        /// </summary>
        [JsonPropertyName("offset")]
        public int? Offset { get; set; }

        /// <summary>
        /// Size of the serialized DV in bytes (raw data size, i.e. before base85 encoding, if inline).
        /// </summary>
        [JsonPropertyName("sizeInBytes")]
        public int? SizeInBytes { get; set; }

        /// <summary>
        /// Number of rows the given DV logically removes from the file.
        /// </summary>
        [JsonPropertyName("cardinality")]
        public long? Cardinality { get; set; }

        [JsonPropertyName("maxRowIndex")]
        public long? MaxRowIndex { get; set; }
    }
}
