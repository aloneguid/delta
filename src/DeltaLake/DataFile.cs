using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Stowage;

namespace DeltaLake {
    public class DataFile : IEquatable<DataFile> {
        public DataFile(IOPath path, long size, Dictionary<string, string>? partitionValues, long timestamp) {
            Path = path;
            Size = size;
            if(partitionValues != null) {
                PartitionValues = partitionValues;
            }

            Timestamp = timestamp.FromUnixTimeMilliseconds();
        }

        public IOPath Path { get; }

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

        private string PartitionValuesToString() {
            if(!IsPartitioned)
                return string.Empty;

            var sb = new StringBuilder();
            foreach((string key, string value) in PartitionValues) {
                sb.Append($" |{key}={value}");
            }
            return sb.ToString();
        }

        public override string ToString() => $"{Path}   {Size.ToFileSizeUiString()} {PartitionValuesToString()}";
    }
}
