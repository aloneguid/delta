using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Avalonia.Controls;
using Avalonia.Threading;
using CommunityToolkit.Mvvm.ComponentModel;

namespace DeltaLake.AvaloniaUI.ViewModels {

    public partial class DataFileViewModel : ViewModelBase {
        public DataFileViewModel(DataFile dataFile) {
            DataFile = dataFile;
        }

        public string SizeDisplay => DataFile.Size.ToFileSizeUiString();

        public string PathDisplay => DataFile.RelativePath.ToString();

        public DataFile DataFile { get; }
    }

    public partial class DeltaDataViewModel : ViewModelBase {

        private Table? _table;

        [ObservableProperty]
        private ObservableCollection<DataFileViewModel>? _dataFiles;

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
                DataFiles = null;
                return;
            }

            IEnumerable<DataFileViewModel> src = Table.DataFiles.Select(f => new DataFileViewModel(f));

            DataFiles = new ObservableCollection<DataFileViewModel>(src);
        }
    }
}
