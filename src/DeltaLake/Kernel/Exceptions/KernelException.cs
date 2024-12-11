using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DeltaLake.Kernel.Exceptions {

    /// <summary>
    /// Thrown when Kernel cannot execute the requested operation due to the operation being invalid or unsupported.
    /// </summary>
    public class KernelException : Exception {
        public KernelException(string message) : base(message) { }
    }
}
