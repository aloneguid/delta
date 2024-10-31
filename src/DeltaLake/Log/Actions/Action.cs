using System.Text.Json;
using DeltaLake.Log.Actions;

namespace DeltaLake.Log.Actions {

    public abstract class Action {
        public ActionType DeltaAction { get; set; }

        protected Action(ActionType action) {
            DeltaAction = action;

            // ModificationTime = _data.ModificationTime!.Value.FromUnixTimeMilliseconds();
        }

        /// <summary>
        /// Creates an action from the raw json object.
        /// For list of actions see https://github.com/delta-io/delta/blob/master/PROTOCOL.md#actions
        /// </summary>
        public static Action CreateFromJsonObject(string name, JsonElement je) {

            //if(name == "commitInfo")
            //    return new CommitInfoAction(je.Deserialize<Dictionary<string, object?>>()!);
            //else if(name == "protocol")
            //    return new ProtocolEvolutionAction(je.Deserialize<ProtocolEvolution>()!);
            //else if(name == "metaData")
            //    return new ChangeMetadataAction(je.Deserialize<ChangeMetadata>()!);
            //else if(name == "add")
            //    return new AddFileAction(je.Deserialize<AddFile>()!);
            //else if(name == "remove")
            //    return new RemoveFileAction(je.Deserialize<RemoveFile>()!);

            throw new NotSupportedException($"action '{name}' is not supported");
        }

        public override string ToString() => DeltaAction.ToString();

        public virtual void Validate() {
            
        }
    }
}
