using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DeltaLake.Kernel.Engine {

    /// <summary>
    /// An interface that encapsulates all the functions needed by Kernel to perform commits to a table
    /// owned by a Commit Coordinator.
    /// Commit coordinator is defined by the Delta Protocol.
    /// See https://github.com/delta-io/delta/blob/master/protocol_rfcs/managed-commits.md#sample-commit-owner-api
    /// </summary>
    public interface ICommitCoordinatorClientHandler {
    }
}
