import com.opencsv.CSVReader;

import java.io.*; // wildcard import for brevity in tutorial
import java.net.*;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.*;

public class JavaOpenSocket {
    private static final Executor SERVER_EXECUTOR = Executors.newSingleThreadExecutor();
    private static final int PORT = 9999;
    private static final String DELIMITER = ":";
    private static final long EVENT_PERIOD_SECONDS = 1;
    private static final Random random = new Random();

    public static void main(String[] args) throws Exception {
        BlockingQueue<String> eventQueue = new ArrayBlockingQueue<>(100);
        SERVER_EXECUTOR.execute(new SteamingServer(eventQueue));
        while (true) {
            eventQueue.put(generateEvent());
            Thread.sleep(TimeUnit.SECONDS.toMillis(EVENT_PERIOD_SECONDS));
        }
    }

    private static String generateEvent() throws Exception {
/*        int userNumber = random.nextInt(10);
        String event = random.nextBoolean() ? "login" : "purchase";
        // In production use a real schema like JSON or protocol buffers
        return String.format("user-%s", userNumber) + DELIMITER + event;*/
        CSVReader reader = new CSVReader(new FileReader("athlete_events.csv"), ',' , '"', 1);
        String[] nextLine;
        while ((nextLine = reader.readNext()) != null) {
            if (nextLine != null) {
                return Arrays.toString(nextLine);
            }
        }
        return null;
    }

    private static class SteamingServer implements Runnable {
        private final BlockingQueue<String> eventQueue;

        public SteamingServer(BlockingQueue<String> eventQueue) {
            this.eventQueue = eventQueue;
        }

        @Override
        public void run() {
            try (ServerSocket serverSocket = new ServerSocket(PORT);
                 Socket clientSocket = serverSocket.accept();
                 PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
            ) {
                while (true) {
                    String event = eventQueue.take();
                    System.out.println(String.format("Writing \"%s\" to the socket.", event));
                    out.println(event);
                }
            } catch (IOException|InterruptedException e) {
                throw new RuntimeException("Server error", e);
            }
        }
    }
}