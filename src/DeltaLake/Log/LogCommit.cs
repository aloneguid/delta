using Stowage;
using Action = DeltaLake.Log.Actions.Action;

namespace DeltaLake.Log {
    public class LogCommit {
        public LogCommit(LogEntry entry) {
            File = entry.Entry;
            Version = entry.Version;
        }

        /// <summary>
        /// File that contains the commit information
        /// </summary>
        public IOEntry File { get; }

        /// <summary>
        /// Table version at the time of the commit
        /// </summary>
        public long Version { get; }

        public List<Action> Actions { get; } = new List<Action>();

        public override string ToString() => $"v{Version} {File.Name} ({Actions.Count})";
    }
}