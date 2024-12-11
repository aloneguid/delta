using System.Text.Json;
using System.Text.Json.Serialization;
using Parquet.Serialization.Attributes;

namespace DeltaLake.Kernel.Internal.Actions {
    class CommitLine {

        [JsonPropertyName("txn")]
        public TransactionIdentifiers? Txn { get; set; }

        [JsonPropertyName("add")]
        public AddFile? Add { get; set; }

        [JsonPropertyName("remove")]
        public RemoveFile? Remove { get; set; }

        [JsonPropertyName("metaData")]
        public Metadata? MetaData { get; set; }

        [JsonPropertyName("protocol")]
        public Protocol? Protocol { get; set; }

        [JsonPropertyName("commitInfo")]
        [ParquetIgnore]
        public JsonElement? Commit { get; set; }

        public Action ToAction() {
            if(Commit != null) {
                return new CommitInfo(Commit.Value);
            } else if(Protocol != null) {
                return Protocol;
            } else if(MetaData != null) {
                return MetaData;
            } else if(Add != null) {
                return Add;
            } else if(Remove != null) {
                return Remove;
            }

            throw new InvalidDataException("Unknown action, all known fields are missing");
        }

        public override string ToString() {

            if(Add != null)
                return "Add: " + Add.Path;

            return "?";
        }
    }
}
