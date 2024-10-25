using System.Text.Json.Serialization;
using DeltaLake.Log.Actions;

namespace DeltaLake.Log {
    class CheckpointPoco {

        [JsonPropertyName("txn")]
        public TransactionIdentifiersPoco? Txn { get; set; }

        [JsonPropertyName("add")]
        public AddFilePoco? Add { get; set; }

        [JsonPropertyName("remove")]
        public RemoveFilePoco? Remove { get; set; }

        [JsonPropertyName("metaData")]
        public ChangeMetadataPoco? MetaData { get; set; }

        [JsonPropertyName("protocol")]
        public ProtocolEvolutionActionPoco? Protocol { get; set; }

        public override string ToString() {

            if(Add != null) return "Add: " + Add.Path;

            return "?";
        }
    }
}
