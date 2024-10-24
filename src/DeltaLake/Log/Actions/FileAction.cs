namespace DeltaLake.Log.Actions {
    public abstract class FileAction : Action {

        protected readonly AddRemoveFilePoco _data;

        protected FileAction(AddRemoveFilePoco data, bool isAdd) : base(isAdd ? DeltaAction.AddFile : DeltaAction.RemoveFile) {
            _data = data;

            if(data.Path == null)
                throw new ArgumentNullException(nameof(data.Path));

            Path = data.Path;
        }

        public string Path { get; init; }

        public override string ToString() => $"{base.ToString()} {Path}";
    }
}