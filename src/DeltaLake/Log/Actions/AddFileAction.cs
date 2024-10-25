namespace DeltaLake.Log.Actions {
    public class AddFileAction : Action {

        protected readonly AddFilePoco _data;

        public AddFileAction(AddFilePoco data) : base(DeltaAction.AddFile) {
            _data = data;

            if(data.Path == null)
                throw new ArgumentNullException(nameof(data.Path));

            Path = data.Path;
        }

        public string Path { get; init; }

        public override string ToString() => $"{base.ToString()} {Path}";
    }
}
