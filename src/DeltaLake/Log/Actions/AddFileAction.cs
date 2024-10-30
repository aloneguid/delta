using DeltaLake.Log.Poco;

namespace DeltaLake.Log.Actions {
    public class AddFileAction : Action {

        private readonly AddFilePoco _data;

        internal AddFileAction(AddFilePoco data) : base(DeltaAction.AddFile) {
            _data = data;

            if(data.Path == null)
                throw new ArgumentNullException(nameof(data.Path));

            if(data.ModificationTime == null)
                throw new ArgumentNullException(nameof(data.ModificationTime));

            Path = data.Path;
            ModificationTime = _data.ModificationTime!.Value.FromUnixTimeMilliseconds();
        }

        /// <summary>
        /// Relative path to the data file.
        /// </summary>
        public string Path { get; init; }

        /// <summary>
        /// The time this logical file was created.
        /// </summary>
        public DateTime ModificationTime { get; init; }

        public override string ToString() => $"add {Path} @ {ModificationTime}";
    }
}
