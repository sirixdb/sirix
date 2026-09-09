import java.io.PrintWriter;
import java.nio.file.Path;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedFrame;
import jdk.jfr.consumer.RecordedMethod;
import jdk.jfr.consumer.RecordedThread;
import jdk.jfr.consumer.RecordingFile;

class ReadSamples {
  public static void main(String[] args) throws Exception {
    try (RecordingFile recording = new RecordingFile(Path.of(args[0]));
         PrintWriter output = new PrintWriter(args[1])) {
      while (recording.hasMoreEvents()) {
        RecordedEvent event = recording.readEvent();
        String type = event.getEventType().getName();
        if (!type.equals("jdk.ExecutionSample") && !type.equals("jdk.NativeMethodSample")) continue;
        RecordedThread sampled = event.getThread("sampledThread");
        if (sampled == null || event.getStackTrace() == null) continue;
        output.print(event.getStartTime().toString());
        output.print('\t');
        output.print(sampled.getJavaName());
        output.print('\t');
        boolean first = true;
        for (RecordedFrame frame : event.getStackTrace().getFrames()) {
          if (!first) output.print(';');
          first = false;
          RecordedMethod method = frame.getMethod();
          output.print(method.getType().getName());
          output.print('.');
          output.print(method.getName());
          output.print(':');
          output.print(frame.getLineNumber());
        }
        output.println();
      }
    }
  }
}
