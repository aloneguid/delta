using Stowage;

namespace DeltaLake.Kernel.Engine.CoordinatedCommits {

    /// <summary>
    /// Representation of a commit file. It contains the version of the commit, the file status of the
    /// commit, and the timestamp of the commit.This is used when we want to get the commit information
    /// from the CommitCoordinatorClientHandler#commit and CommitCoordinatorClientHandler#getCommits APIs.
    /// </summary>
    public record Commit(long Version, IOEntry FileStatus, DateTime CommitTimestamp);
}
