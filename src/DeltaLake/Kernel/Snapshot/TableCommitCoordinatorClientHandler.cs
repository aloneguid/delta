using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace DeltaLake.Kernel.Snapshot {
    /// <summary>
    /// A wrapper around <see cref="ICommitCoordinatorClientHandler"/> that provides a more user-friendly API
    /// for committing/accessing commits to a specific table.This class takes care of passing the table
    /// specific configuration to the underlying { @link CommitCoordinatorClientHandler }
    /// e.g.logPath/coordinatedCommitsTableConf.
    /// </summary>
    public class TableCommitCoordinatorClientHandler {
    }
}
