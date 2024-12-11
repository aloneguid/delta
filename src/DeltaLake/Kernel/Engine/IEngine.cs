using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Stowage;

namespace DeltaLake.Kernel.Engine {

    /// <summary>
    /// <see cref="IEngine"/> boostraps the <see cref="Table"/> object, allowing you to plug in your own library for file storage,
    /// JSON parsing, reading Parquet files etc.
    /// </summary>
    public interface IEngine {
        public IFileStorage FileStorage { get; }

        /// <summary>
        /// List the paths in the same directory that are lexicographically greater or equal to (UTF-8 sorting)
        /// the given `path`. The result should also be sorted by the file name.
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public Task<IReadOnlyCollection<IOEntry>> ListFrom(IOPath path, string prefix);
    }
}
