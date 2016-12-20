package brave;

import brave.internal.Nullable;
import java.util.concurrent.ConcurrentHashMap;
import zipkin.Endpoint;
import zipkin.reporter.Reporter;

/**
 * Offset-based recorder: Uses a single point of reference and offsets to create annotation
 * timestamps.
 *
 * <p>Method signatures are based on the zipkin 2 model eventhough it isn't out, yet.
 */
final class SpanRecorder {
  final Endpoint localEndpoint;
  final Reporter<zipkin.Span> reporter;
  final Ticker ticker;

  // epochMicros is derived by this
  final long createTimestamp;
  final long createTick;

  // TODO: add bookkeeping so that spans which are perpetually unfinished don't OOM
  // For example, finagle has a flusher thread to report spans that lived longer than 2 minutes
  final ConcurrentHashMap<TraceContext, MutableSpan> spanMap = new ConcurrentHashMap(64);

  SpanRecorder(Endpoint localEndpoint, Reporter<zipkin.Span> reporter, Ticker ticker, Clock clock) {
    this.localEndpoint = localEndpoint;
    this.reporter = reporter;
    this.ticker = ticker;
    this.createTimestamp = clock.epochMicros();
    this.createTick = ticker.tickNanos();
  }

  /** gets a timestamp based on this the create tick. */
  long epochMicros() {
    return ((ticker.tickNanos() - createTick) / 1000) + createTimestamp;
  }

  void start(TraceContext context, long timestamp) {
    MutableSpan span = getSpan(context).start(timestamp);
    maybeFinish(context, span);
  }

  void name(TraceContext context, String name) {
    MutableSpan span = getSpan(context).name(name);
    maybeFinish(context, span);
  }

  void kind(TraceContext context, Span.Kind kind) {
    MutableSpan span = getSpan(context).kind(kind);
    maybeFinish(context, span);
  }

  void annotate(TraceContext context, long timestamp, String value) {
    MutableSpan span = getSpan(context).annotate(timestamp, value);
    maybeFinish(context, span);
  }

  void tag(TraceContext context, String key, String value) {
    MutableSpan span = getSpan(context).tag(key, value);
    maybeFinish(context, span);
  }

  void remoteEndpoint(TraceContext context, Endpoint remoteEndpoint) {
    MutableSpan span = getSpan(context).remoteEndpoint(remoteEndpoint);
    maybeFinish(context, span);
  }

  void finish(TraceContext context, @Nullable Long finishTimestamp, @Nullable Long duration) {
    MutableSpan span = getSpan(context).finish(finishTimestamp, duration);
    maybeFinish(context, span);
  }

  MutableSpan getSpan(TraceContext context) {
    MutableSpan span = spanMap.get(context);
    if (span != null) return span;

    MutableSpan newSpan = new MutableSpan(context, localEndpoint);
    MutableSpan prev = spanMap.putIfAbsent(context, newSpan);
    return prev != null ? prev : newSpan;
  }

  void maybeFinish(TraceContext context, MutableSpan span) {
    if (!span.isFinished()) return;
    synchronized (span) {
      spanMap.remove(context, span);
      reporter.report(span.toSpan());
    }
  }
}
