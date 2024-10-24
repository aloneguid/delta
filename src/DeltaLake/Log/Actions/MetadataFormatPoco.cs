﻿namespace Delta.Net.Log.Actions {
    public class MetadataFormatPoco {
        /// <summary>
        /// Name of the encoding for files in this table
        /// </summary>
        public string Provider { get; set; } = "parquet";

        /// <summary>
        /// A map containing configuration options for the format
        /// </summary>
        public Dictionary<string, string> Options { get; set; } = new Dictionary<string, string>();
    }
}
