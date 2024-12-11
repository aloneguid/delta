namespace DeltaLake.Kernel.Internal.Actions {
    public enum ActionType {
        /// <summary>
        /// Commit Provenance Information: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#commit-provenance-information
        /// </summary>
        CommitInfo,

        /// <summary>
        /// Protocol Evolution: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#protocol-evolution
        /// </summary>
        Protocol,

        /// <summary>
        /// Change metadata: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#change-metadata
        /// </summary>
        Metadata,

        /// <summary>
        /// Add individual logical file: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-file-and-remove-file
        /// </summary>
        AddFile,

        /// <summary>
        /// Remove individual logical file: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-file-and-remove-file
        /// </summary>
        RemoveFile
    }
}
