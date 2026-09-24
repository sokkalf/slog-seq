[![Go Reference](https://pkg.go.dev/badge/github.com/sokkalf/slog-seq.svg)](https://pkg.go.dev/github.com/sokkalf/slog-seq)
![CI tests](https://github.com/sokkalf/slog-seq/actions/workflows/tests.yml/badge.svg)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)

# slog-seq

**slog-seq** is a library for sending logs to a [Seq](https://datalust.co/seq) server, as a handler for Go's structured logging [slog](https://go.dev/blog/slog).

It also supports some trace functionality.

## Installation

```bash
go get github.com/sokkalf/slog-seq
```

## Quick start

It's pretty easy to get going.

```go
seqLogger, handler := slogseq.NewLogger("http://your-seq-server/ingest/clef",
    slogseq.WithAPIKey("your-api-key"),
    slogseq.WithBatchSize(50),
    slogseq.WithFlushInterval(2*time.Second),
)
defer handler.Close()

slog.SetDefault(seqLogger)
slog.Info("Hello, world!")
```

You can set some options, here are some examples:

```go
opts := &slog.HandlerOptions{
	Level:     slog.LevelInfo,  // minimum log level
	AddSource: true,            // show source file, line and function in log
	ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
		if a.Key == "password" {
			// mask passwords
			a.Value = slog.StringValue("*****")
		}
		if a.Key == slog.SourceKey {
			// Replace the full path with just the file name
			s := a.Value.Any().(*slog.Source)
			s.File = path.Base(s.File)
		}
		return a
	},
}
```

and then pass it to the `NewLogger` function with `slogseq.WithHandlerOptions(opts)`.

For the `AddSource` option, the default key used is `slog.SourceKey` ("source"), but you can change it by using `slogseq.WithSourceKey("your-key")` if this key is already used for something else.
If you log something else with this key when AddSource is enabled, it will be overwritten.

## HTTP client

If you need to disable TLS certificate verification, you can do so by using the option `slogseq.WithInsecure()`.

Alternatively, you can provide your own HTTP client by using the option `slogseq.WithHTTPClient(client)`.

## Multiple workers

You can set the number of workers that will send logs to the Seq server by using the option `slogseq.WithWorkers(n)`.

This can be useful if you have a high enough volume of logs to cause dropped messages.

## Traces

`LoggingSpanProcessor` implements a `trace.SpanProcessor` that sends spans to Seq using either `trace.NewSimpleSpanProcessor` or `trace.NewBatchSpanProcessor`, which behaves pretty much the same as slog-seq already handles batching.

Here is an example of how to use it:

```go
spanProcessor := trace.NewSimpleSpanProcessor(&slogseq.LoggingSpanProcessor{Handler: handler})
tp := trace.NewTracerProvider(trace.WithSpanProcessor(spanProcessor), trace.WithSampler(trace.AlwaysSample()))
tracer := tp.Tracer("example-tracer")
ctx := context.Background()
spanCtx, span := tracer.Start(ctx, "operation")
span.AddEvent("Starting work")
time.Sleep(500 * time.Millisecond)
slog.InfoContext(spanCtx, "This is a span log message", "key", "value")
spanCtx, subSpan := tracer.Start(spanCtx, "sub operation")
subSpan.AddEvent("Sub operation started")
time.Sleep(500 * time.Millisecond)
subSpan.AddEvent("Sub operation completed", tr.WithAttributes(attribute.String("key", "value")))
subSpan.End()
span.AddEvent("Work done")
slog.InfoContext(spanCtx, "All done!")
span.End()
```

![Seq with traces](../master/doc/seq_screenshot.png)

## License

MIT
