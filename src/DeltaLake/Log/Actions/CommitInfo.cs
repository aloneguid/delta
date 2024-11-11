using System.Text.Json;

namespace DeltaLake.Log.Actions {
    /// <summary>
    /// Commit Provenance Information.
    /// A delta file can optionally contain additional provenance information about what higher-level operation was being performed as well as who executed it.Implementations are free to store any valid JSON-formatted data via the commitInfo action.
    /// </summary>
    public class CommitInfo : Action {
        public CommitInfo(JsonElement je) : base(ActionType.CommitInfo) {
        }
    }
}
