import java.nio.file.Path;
import java.io.PrintWriter;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedFrame;
import jdk.jfr.consumer.RecordedMethod;
import jdk.jfr.consumer.RecordedThread;
import jdk.jfr.consumer.RecordingFile;

class ReadMainSamples {
  public static void main(String[] args) throws Exception {
    try (RecordingFile recording = new RecordingFile(Path.of(args[0]));
         PrintWriter output = new PrintWriter(args[1])) {
      while (recording.hasMoreEvents()) {
        RecordedEvent event = recording.readEvent();
        if (!event.getEventType().getName().equals("jdk.ExecutionSample")) continue;
        RecordedThread sampled = event.getThread("sampledThread");
        if (sampled == null || !"main".equals(sampled.getJavaName()) || event.getStackTrace() == null) continue;
        output.print(event.getStartTime().toString());
        output.print('\t');
        boolean first = true;
        for (RecordedFrame frame : event.getStackTrace().getFrames()) {
          if (!first) output.print(';');
          first = false;
          RecordedMethod method = frame.getMethod();
          output.print(method.getType().getName());
          output.print('.');
          output.print(method.getName());
        }
        output.println();
      }
    }
  }
}
