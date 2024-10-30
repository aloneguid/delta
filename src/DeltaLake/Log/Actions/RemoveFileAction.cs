using DeltaLake.Log.Poco;

namespace DeltaLake.Log.Actions {
    public class RemoveFileAction : Action {

        private readonly RemoveFilePoco _data;
        internal RemoveFileAction(RemoveFilePoco data) : base(DeltaAction.RemoveFile) {
            _data = data;

            if(data.Path == null)
                throw new ArgumentNullException(nameof(data.Path));

            Path = data.Path;
        }

        public string Path { get; init; }

        public override string ToString() => $"{base.ToString()} {Path}";
    }
}
