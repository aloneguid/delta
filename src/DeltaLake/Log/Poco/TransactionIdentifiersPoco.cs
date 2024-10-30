using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace DeltaLake.Log.Poco {
     class TransactionIdentifiersPoco {

        /// <summary>
        /// A unique identifier for the application performing the transaction
        /// </summary>
        [JsonPropertyName("appId")]
        public string? AppId { get; set; }

        /// <summary>
        /// An application-specific numeric identifier for this transaction
        /// </summary>
        [JsonPropertyName("version")]
        public long? Version { get; set; }

        /// <summary>
        /// The time when this transaction action is created, in milliseconds since the Unix epoch
        /// </summary>
        [JsonPropertyName("lastUpdated")]
        public long? LastUpdated { get; set; }

    }
}
