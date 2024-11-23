using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Avalonia.Data.Converters;

namespace DeltaLake.AvaloniaUI.Converters {
    public class GenericConverter : IValueConverter {
        public object? Convert(object? value, Type targetType, object? parameter, CultureInfo culture) {
            if(value is string[]) {
                return string.Join(", ", (string[])value);
            } else if(value is IDictionary<string, string> dict) {
                return string.Join("\n", dict.Select(kv => $"{kv.Key}: {kv.Value}"));
            }

            return null;
        }
        public object? ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture) => throw new NotImplementedException();
    }
}
