using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using Avalonia.Controls;
using CommunityToolkit.Mvvm.ComponentModel;
using DeltaLake.Log;
using Stowage;

namespace DeltaLake.AvaloniaUI.ViewModels {

    public partial class FolderIOEntryViewModel : ViewModelBase {
        private readonly MainWindowViewModel _parent;

        public IOEntry Entry { get; }

        public FolderIOEntryViewModel(MainWindowViewModel parent, IOEntry entry) {
            _parent = parent;
            Entry = entry;
        }

        public void Activate() {
            _parent.NavigateToFolder(Entry);
        }
    }

    public partial class MainWindowViewModel : ViewModelBase {

        private readonly IFileStorage _fs;

        [ObservableProperty]
        private string? _implRoot;

        [ObservableProperty]
        private bool _isBrowserOpen = true;

        [ObservableProperty]
        private IOPath _fsPath = IOPath.Root;

        [ObservableProperty]
        private ObservableCollection<FolderIOEntryViewModel>? _pathEntries;

        [ObservableProperty]
        private bool _isDeltaTablePath;

        [ObservableProperty]
        private Table? _deltaTable;

        [ObservableProperty]
        private long _selectedDeltaVersion;

        [ObservableProperty]
        private ObservableCollection<long>? _deltaVersions;

        [ObservableProperty]
        private ObservableCollection<LogCommit>? _deltaHistory;

        [ObservableProperty]
        private LogCommit? _selectedLogCommit;

        [ObservableProperty]
        private ObservableCollection<DataFile>? _dataFiles;

        [ObservableProperty]
        private DataFile? _selectedDataFile;

        public MainWindowViewModel() {
            _fs = CreateFileStorage(out string implRoot);
            ImplRoot = implRoot;

#if DEBUG
            // Design time data
            if(Design.IsDesignMode) {
                const string path = "D:\\delta-dotnet\\src\\DeltaLake.Test\\data\\chinook\\artist.simple";
                _fs = Files.Of.LocalDisk(path);
                ImplRoot = path;
            }
#endif
            LoadFSPathAsync().Forget();
        }

        private async Task LoadFSPathAsync() {
            if(FsPath == null)
                return;

            IReadOnlyCollection<IOEntry> entries = await _fs.Ls(FsPath);
            PathEntries = new ObservableCollection<FolderIOEntryViewModel>(entries
                .Where(e => e.Path.IsFolder)
                .OrderBy(e => e.Name)
                .Select(e => new FolderIOEntryViewModel(this, e)));
            string fn = DeltaLog.DeltaLogDirName + IOPath.PathSeparatorString;
            IsDeltaTablePath = PathEntries.Any(e => e.Entry.Name == fn);

            if(IsDeltaTablePath) {
                DeltaTable = await Table.OpenAsync(_fs, FsPath);
                DeltaVersions = new ObservableCollection<long>(DeltaTable.Versions.OrderDescending());
                SelectedDeltaVersion = DeltaVersions.First();
                DataFiles = new ObservableCollection<DataFile>(DeltaTable.DataFiles);
                SelectedDataFile = DataFiles.FirstOrDefault();

                DeltaHistory = new ObservableCollection<LogCommit>(DeltaTable.History);
                SelectedLogCommit = DeltaHistory.FirstOrDefault();
            } else {
                DeltaTable = null;
                DeltaVersions = null;
                SelectedDeltaVersion = 0;
                DataFiles = null;
                SelectedDataFile = null;

                DeltaHistory = null;
                SelectedLogCommit = null;
            }
        }

        public void NavigateToFolder(IOEntry entry) {
            FsPath = entry.Path;
            Refresh();
        }

        public void Refresh() {
            LoadFSPathAsync().Forget();
        }

        public void NavigateToParentFolder() {
            FsPath = FsPath.Parent;
            Refresh();
        }

        public void ToggleBrowserOpen() {
            IsBrowserOpen = !IsBrowserOpen;
        }

        /// <summary>
        /// Creates root filesystem instance based on command line arguments.
        /// If arguments were invalid or no arguments are passed, root filesystem will be created
        /// as local disk and pointed to current directory.
        /// </summary>
        /// <returns></returns>
        private static IFileStorage CreateFileStorage(out string implRoot) {
            string[] args = Environment.GetCommandLineArgs();

            if(args.Length > 1) {   // first arg is executable name, so ignore it
                string connectionString = args[1];

                try {
                    IFileStorage ifs = Files.Of.ConnectionString(connectionString);
                    implRoot = connectionString;
                    return ifs;
                } catch(Exception ex) {
                    Console.WriteLine($"Invalid connection string: {connectionString}");
                    Console.WriteLine(ex.Message);
                }

                // last chance - treat argument as local disk path
                try {
                    IFileStorage ifs = Files.Of.LocalDisk(args[1]);
                    implRoot = args[1];
                    return ifs;
                } catch(Exception ex) {
                    Console.WriteLine($"Invalid local disk path: {args[1]}");
                    Console.WriteLine(ex.Message);
                }
            }

            implRoot = Environment.CurrentDirectory;
            return Files.Of.LocalDisk(Environment.CurrentDirectory);
        }
    }
}
