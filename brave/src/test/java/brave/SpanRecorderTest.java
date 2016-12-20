package brave;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SpanRecorderTest {
  @Test public void relativeTimestamp_incrementsAccordingToNanoTick() {
    AtomicLong tick = new AtomicLong();
    SpanRecorder recorder = Tracer.builder()
        .clock(() -> 0)
        .ticker(tick::getAndIncrement)
        .build().spanRecorder;

    tick.set(1000); // 1 microsecond

    assertThat(recorder.epochMicros()).isEqualTo(1);
  }
}
