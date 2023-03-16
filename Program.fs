module Program

open Propulsion.Internal
open Serilog
open System

[<EntryPoint>]
let main _ =

  let log: ILogger = LoggerConfiguration().WriteTo.Console().CreateLogger()

  let stats =
    { new Propulsion.Streams.Stats<_>(log, TimeSpan.FromMinutes 1, TimeSpan.FromMinutes 1) with
        member _.HandleOk x = ()
        member _.HandleExn(log, x) = () }

  let checkpoints =
    Propulsion.MessageDb.ReaderCheckpoint.CheckpointStore(
      "Host=localhost; Port=5432; Username=postgres; Password=postgres; Database=message_store",
      "message_store",
      "LocalTest2",
      (TimeSpan.FromSeconds 5)
    )
    
  checkpoints.CreateSchemaIfNotExists() |> Async.AwaitTask |> Async.RunSynchronously

  let handle (stream: FsCodec.StreamName) (events: Propulsion.Streams.Default.StreamSpan) _ct =
    task {
      log.Information("Handling {stream_name}", stream)
      return struct (Propulsion.Streams.SpanResult.AllProcessed, ())
    }

  use sink =
    Propulsion.Streams.Default.Config.Start(log, 100, 1, handle, stats, TimeSpan.FromMinutes 1)

  use client = EventStore.Client.EventStoreClient(EventStore.Client.EventStoreClientSettings.Create("esdb://localhost:2113?tls=false"))

  let source =
    Propulsion.EventStoreDb.EventStoreSource(log, TimeSpan.FromMinutes 1, client, 500L, TimeSpan.FromMilliseconds 200, checkpoints, sink)

  use src = source.Start()
  src.AwaitWithStopOnCancellation() |> Async.RunSynchronously
  0

