namespace DeltaLake.Log.Actions {
    public class AddFileAction : FileAction {
        public AddFileAction(AddRemoveFilePoco data) : base(data, true) {
        }

    }
}
