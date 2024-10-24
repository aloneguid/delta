namespace DeltaLake.Log.Actions {
    public class RemoveFileAction : FileAction {
        public RemoveFileAction(AddRemoveFilePoco data) : base(data, false) {
        }
    }
}
