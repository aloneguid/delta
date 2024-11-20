using System.Text;
using DeltaLake.Log.Actions;
using Stowage;

namespace DeltaLake {
    public class DataFile : IEquatable<DataFile> {

        public DataFile(FileBase fileAction, IOPath tablePath, IOPath relativePath) {

            BaseAction = fileAction;

            if(fileAction.Size == null)
                throw new ArgumentException("Size is required for DataFile");
            if(fileAction.Timestamp == null)
                throw new ArgumentException("Timestamp is required for DataFile");

            TablePath = tablePath;
            RelativePath = relativePath;
            Path = new IOPath(tablePath, relativePath);
            Size = fileAction.Size.Value;
            if(fileAction.PartitionValues != null) {
                PartitionValues = fileAction.PartitionValues;
            }
            Timestamp = fileAction.Timestamp.Value.FromUnixTimeMilliseconds();
        }

        internal FileBase BaseAction { get; init; }

        public IOPath TablePath { get; }

        public IOPath Path { get; }

        public IOPath RelativePath { get; }

        public long Size { get; }

        public IDictionary<string, string> PartitionValues { get; } = new Dictionary<string, string>();

        public bool IsPartitioned => PartitionValues.Count > 0;

        public DateTime Timestamp { get; }

        public bool Equals(DataFile? other) {
            if(other is null) {
                return false;
            }

            return Path.Equals(other.Path);
        }

        public override bool Equals(object? obj) => Equals(obj as DataFile);

        public override int GetHashCode() => Path.GetHashCode();

        private string PartitionValuesToString() => string.Join('|', PartitionValues.Select((k, v) => $"{k}={v}"));

        public override string ToString() => $"{Path} ({Size.ToFileSizeUiString()}){PartitionValuesToString()}";
    }
}
