# Delta.IO [![NuGet Version](https://img.shields.io/nuget/vpre/DeltaIO?style=flat-square)](https://www.nuget.org/packages/DeltaIO)


![](logo.svg)

This is an ongoing attempt to implement [delta.io](https://delta.io/) in pure .net with no native dependencies, wrappers and so on.

## Why implement the Delta Lake transaction log protocol in .NET?

Delta Spark depends on Java and Spark, which is fine for many use cases, but not all Delta Lake users want to depend on these libraries. DearDeltaLake allows using Delta Lake in C# or other languages using .NET runtime.

DearDeltaLake lets you query Delta tables without depending on Java/Scala.

Suppose you want to query a Delta table with on your local machine. DearDeltaLake makes it easy to query the table with a simple `dotnet add package` command - no need to install Java or Spark.

## Status

Still early stages.

I can already read and parse delta log, with some understanding of it's file structure.

## Quick start

After installing the nuget package [![NuGet Version](https://img.shields.io/nuget/vpre/DeltaIO?style=flat-square)](https://www.nuget.org/packages/DeltaIO), find out type of storage your tables are stored in. We will stick with local disk here, but there are plenty of other options.

```csharp
using DeltaLake;

// open table from the local disk
Table table = new Table("c:/table/folder");
```

## Reading

Delta table essentially consists of different parquet files, and to read the table, you essentially need to figure out which parquet files constitute the current version of a delta table. To get the list of those files:

```csharp
IReadOnlyCollection<string> files = await table.GetFilesAsync();
```

This returns the list of files at the *latest* version of this table.

## Appending

todo

## Deleting

todo

## Contributing

Bookmark, star, start discussions if you are interested in the future of this project!

## Useful Links

- [Delta Transaction Log Protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md).
- [Delta Kernel](https://docs.delta.io/latest/delta-kernel.html).
- [Integration Test Dataset](https://github.com/delta-io/delta-rs/tree/main/crates/test/tests/data).
- Alternative implementations
  - [delta-net](https://github.com/johnsusi/delta-net)
  - [delta-dotnet](https://github.com/delta-incubator/delta-dotnet)
- [Chinook database](https://github.com/lerocha/chinook-database) used in generating test delta tables.
