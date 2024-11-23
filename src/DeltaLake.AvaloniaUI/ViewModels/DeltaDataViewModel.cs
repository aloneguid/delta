using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Avalonia.Controls;
using Avalonia.Controls.Models.TreeDataGrid;
using Avalonia.Threading;
using CommunityToolkit.Mvvm.ComponentModel;

namespace DeltaLake.AvaloniaUI.ViewModels {

    public class PartitionNode {

        public PartitionNode(DataFile dataFile) {
            Name = dataFile.RelativePath.ToString();
            DataFile = dataFile;
        }

        public PartitionNode(Table table, int partitionIndex, Dictionary<string, string> partitionFilter) {
            Name = table.PartitionColumns.Skip(partitionIndex).Take(1).Single();

            // filter data files using passed partition filter
            var dfs = table.DataFiles
                .Where(df => ContainsSubdictionary(df.PartitionValues, partitionFilter))
                .ToList();

            // get list of possible values for this partition
            var values = dfs.Select(df => df.PartitionValues[Name]).Distinct().ToList();

            // create child nodes for each value
            foreach(string value in values) {
                var filter = new Dictionary<string, string>(partitionFilter) { { Name, value } };

                // if this is the last partition, values are data files
                if(partitionIndex == table.PartitionColumns.Count - 1) {
                    // filter data files using passed partition filter
                    var dataFiles = table.DataFiles
                        .Where(df => ContainsSubdictionary(df.PartitionValues, filter))
                        .ToList();

                    Children.Add(new PartitionNode(value, dataFiles));
                } else {
                    Children.Add(new PartitionNode(table, partitionIndex + 1, filter));
                }

            }
        }

        public PartitionNode(string partitionValue, IEnumerable<DataFile> dataFiles) {
            Name = partitionValue;

            foreach(DataFile df in dataFiles) {
                Children.Add(new PartitionNode(df));
            }
        }

        private static bool ContainsSubdictionary(IDictionary<string, string> master, IDictionary<string, string> sub) {
            return sub.All(kv => master.ContainsKey(kv.Key) && master[kv.Key] == kv.Value);
        }   

        public string Name { get;}

        public bool IsExpanded { get; set; } = true;

        public DataFile? DataFile { get; }

        public List<PartitionNode> Children { get; set; } = new();
    }

    public partial class DeltaDataViewModel : ViewModelBase {

        private Table? _table;

        [ObservableProperty]
        private HierarchicalTreeDataGridSource<PartitionNode>? _partitionNodes;

        public DeltaDataViewModel() {
#if DEBUG
            if(Design.IsDesignMode) {
                LoadTableAsync().Forget();
            }
#endif
        }

#if DEBUG
        public async Task LoadTableAsync() {
            Table tbl = await DebugDataLoader.LoadTableAsync();

            await Dispatcher.UIThread.InvokeAsync(() => {
                Table = tbl;
            });
        }
#endif

        public Table? Table {
            get => _table;
            set {
                _table = value;

                BuildTreeViewModel();
            }
        }

        private void BuildTreeViewModel() {
            if(Table == null) {
                PartitionNodes = null;
                return;
            }

            IEnumerable<PartitionNode> src;

            if(Table.IsPartitioned) {
                src = new List<PartitionNode> { new PartitionNode(Table, 0, new Dictionary<string, string>()) };
            } else {
                src = Table.DataFiles.Select(df => new PartitionNode(df));
            }

            PartitionNodes = new HierarchicalTreeDataGridSource<PartitionNode>(src) {
                Columns = {
                    new HierarchicalExpanderColumn<PartitionNode>(
                    new TextColumn<PartitionNode, string>("Name", x => x.Name),
                    x => x.Children, isExpandedSelector: x => x.IsExpanded),
                    new TextColumn<PartitionNode, string>("Size", x => x.DataFile == null? "" : x.DataFile.Size.ToFileSizeUiString()),
                }
            };

        }
    }
}
