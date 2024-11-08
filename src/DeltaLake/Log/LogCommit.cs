using Stowage;
using Action = DeltaLake.Log.Actions.Action;

namespace DeltaLake.Log {
    public class LogCommit {
        public LogCommit(IOEntry file) {
            File = file;
        }

        public IOEntry File { get; }

        public List<Action> Actions { get; } = new List<Action>();
    }
}