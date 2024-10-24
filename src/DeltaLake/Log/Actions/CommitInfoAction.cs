namespace DeltaLake.Log.Actions {
    /// <summary>
    /// Additional provenance information about what higher-level operation was being performed as well as who executed it.
    /// Implementations are free to store any valid JSON-formatted data via the commitInfo action.
    /// </summary>
    public class CommitInfoAction : Action {
        public CommitInfoAction(Dictionary<string, object?> data) : base(DeltaAction.CommitInfo) {
            Data = data;
        }

        public Dictionary<string, object?> Data { get; }
    }
}
