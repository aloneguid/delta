using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DeltaLake.Kernel.Engine;
using DeltaLake.Kernel.Snapshot;
using Stowage;

/**
 * Java migration notes:
 * - Clock is removed as not needed in C#.
 * - Table and TableImpl merged into one class.
 * - TableImpl constructor replaces all factory methods.
 */


namespace DeltaLake.Kernel {

    /// <summary>
    /// delta-io/delta/kernel/kernel-api/src/main/java/io/delta/kernel/internal/TableImpl.java
    /// </summary>
    public class Table {

        private readonly SnapshotManager _snapshotManager;

        /// <summary>
        /// Instantiate a table object for the Delta Lake table at the given path.
        /// </summary>
        /// <param name="path"></param>
        public Table(IOPath path) {
            Path = path;
            var logPath = new IOPath(path, "_delta_log/");
            _snapshotManager = new SnapshotManager(logPath, path);
        }


        /**
         * The fully qualified path of this table instance.
         */
        public IOPath Path { get; init; }

        /// <summary>
        /// Get the latest snapshot of the table.
        /// </summary>
        /// <param name="engine"></param>
        /// <returns></returns>
        public async Task<TableSnapshot> GetLatestSnapshotAsync(IEngine engine) {
            return await _snapshotManager.BuildLatestSnapshot(engine);
        }

        /**
         * Get the snapshot at the given {@code versionId}.
         *
         * @param engine {@link Engine} instance to use in Delta Kernel.
         * @param versionId snapshot version to retrieve
         * @return an instance of {@link Snapshot}
         * @throws TableNotFoundException if the table is not found
         * @throws KernelException if the provided version is less than the first available version or
         *     greater than the last available version
         * @since 3.2.0
         */
        //Snapshot getSnapshotAsOfVersion(Engine engine, long versionId) throws TableNotFoundException;

        /**
         * Get the snapshot of the table at the given {@code timestamp}. This is the latest version of the
         * table that was committed before or at {@code timestamp}.
         *
         * <p>Specifically:
         *
         * <ul>
         *   <li>If a commit version exactly matches the provided timestamp, we return the table snapshot
         *       at that version.
         *   <li>Else, we return the latest commit version with a timestamp less than the provided one.
         *   <li>If the provided timestamp is less than the timestamp of any committed version, we throw
         *       an error.
         *   <li>If the provided timestamp is after (strictly greater than) the timestamp of the latest
         *       version of the table, we throw an error
         * </ul>
         *
         * .
         *
         * @param engine {@link Engine} instance to use in Delta Kernel.
         * @param millisSinceEpochUTC timestamp to fetch the snapshot for in milliseconds since the unix
         *     epoch
         * @return an instance of {@link Snapshot}
         * @throws TableNotFoundException if the table is not found
         * @throws KernelException if the provided timestamp is before the earliest available version or
         *     after the latest available version
         * @since 3.2.0
         */
        //Snapshot getSnapshotAsOfTimestamp(Engine engine, long millisSinceEpochUTC)
      //throws TableNotFoundException;

        /**
         * Create a {@link TransactionBuilder} which can create a {@link Transaction} object to mutate the
         * table.
         *
         * @param engine {@link Engine} instance to use.
         * @param engineInfo information about the engine that is making the updates.
         * @param operation metadata of operation that is being performed. E.g. "insert", "delete".
         * @return {@link TransactionBuilder} instance to build the transaction.
         * @since 3.2.0
         */
        //TransactionBuilder createTransactionBuilder(
            //Engine engine, String engineInfo, Operation operation);

        /**
         * Checkpoint the table at given version. It writes a single checkpoint file.
         *
         * @param engine {@link Engine} instance to use.
         * @param version Version to checkpoint.
         * @throws TableNotFoundException if the table is not found
         * @throws CheckpointAlreadyExistsException if a checkpoint already exists at the given version
         * @throws IOException for any I/O error.
         * @since 3.2.0
         */
        //void Checkpoint(Engine engine, long version) {
        //    throw new NotImplementedException();
        //}
    }

}
