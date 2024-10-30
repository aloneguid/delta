using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DeltaLake {
    static class Extensions {
        public static DateTime FromUnixTimeMilliseconds(this long millisecondsSinceEpoch) {
            return DateTimeOffset.FromUnixTimeMilliseconds(millisecondsSinceEpoch).UtcDateTime;
        }
    }
}