using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DeltaLake.Kernel.Internal.Actions;
using IronCompress;

namespace DeltaLake.Kernel.Util {

    public enum ColumnMappingMode {
        None,
        Id,
        Name
    }

    static class ColumnMapping {

        public const string COLUMN_MAPPING_MODE_KEY = "delta.columnMapping.mode";

        /// <summary>
        /// Checks if the given column mapping mode in the given table metadata is supported. Throws on
        /// unsupported modes.
        /// </summary>
        /// <param name="metadata"></param>
        public static void ThrowOnUnsupportedColumnMappingMode(Metadata metadata) {
            GetColumnMappingMode(metadata.Configuration);
        }

        /// <summary>
        /// Converts the given column mapping mode name to the corresponding enum value.
        /// </summary>
        /// <param name="name">Case insensitive name</param>
        /// <returns></returns>
        private static ColumnMappingMode ColumnMappingModeFromName(string name) {
            switch(name.ToLower()) {
                case "none":
                    return ColumnMappingMode.None;
                case "id":
                    return ColumnMappingMode.Id;
                case "name":
                    return ColumnMappingMode.Name;
                default:
                    throw new ArgumentException("Unsupported column mapping mode: " + name);
            }
        }

        public static ColumnMappingMode GetColumnMappingMode(Dictionary<string, string>? configuration) {
            if(configuration != null && configuration.TryGetValue(COLUMN_MAPPING_MODE_KEY, out string? mode)) {
                return ColumnMappingModeFromName(mode);
            }

            return ColumnMappingMode.None;
        }
    }
}
