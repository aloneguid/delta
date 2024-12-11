using System.Text.Json;
using Stowage;
using Action = DeltaLake.Kernel.Internal.Actions.Action;
using DeltaLake.Kernel.Engine;
using DeltaLake.Kernel.Internal.Actions;

namespace DeltaLake.Kernel.Internal.Replay {
    class ActionsIterator : IAsyncEnumerable<Action>, IAsyncEnumerator<Action> {
        private readonly LinkedList<DeltaLogFile> _allFiles;
        private Action? _currentAction;
        private readonly LinkedList<Action> _actions = new();
        private readonly IEngine _engine;

        public ActionsIterator(IEnumerable<IOEntry> logEntries, IEngine engine) {
            _allFiles = new LinkedList<DeltaLogFile>(logEntries.Select(DeltaLogFile.FromCommitOrCheckpoint));
            _engine = engine;
        }

        public async ValueTask<bool> MoveNextAsync() {

            while(_actions.Any() || _allFiles.Any()) {

                if(_actions.Any()) {
                    _currentAction = _actions.First!.Value;
                    _actions.RemoveFirst();
                    return true;
                }

                if(_allFiles.Any()) {
                    DeltaLogFile file = _allFiles.First!.Value;
                    _allFiles.RemoveFirst();

                    switch(file.LogType) {
                        case DeltaLogFileType.Commit:
                            await foreach(Action action in ReadJsonAsCommitAsync(file)) {
                                _actions.AddLast(action);
                            }
                            break;
                        default:
                            throw new IOException("Unrecognized log type: " + file.LogType);
                    }
                }
            }

            return false;
        }

        private async IAsyncEnumerable<Action> ReadJsonAsCommitAsync(DeltaLogFile deltaFile) {
            string? content = await _engine.FileStorage.ReadText(deltaFile.File.Path);
            if(content == null)
                throw new InvalidOperationException();

            foreach(string jsonLineRaw in content.Split('\n')) {
                string jsonLine = jsonLineRaw.Trim();
                if(string.IsNullOrEmpty(jsonLine))
                    continue;

                CommitLine? cl = JsonSerializer.Deserialize<CommitLine>(jsonLine);

                if(cl == null)
                    throw new ApplicationException("unparseable action: " + jsonLine);

                yield return cl.ToAction();
            }
        }


        public Action Current => _currentAction ?? throw new ArgumentException();


        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public IAsyncEnumerator<Action> GetAsyncEnumerator(CancellationToken cancellationToken = default) => this;
    }
}
