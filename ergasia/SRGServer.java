import java.io.*;
import java.net.*;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.*;

public class SRGServer {
 
    private static final int DEFAULT_PORT   = 7000;
    private static final int BUFFER_SIZE    = 100;  // max αριθμοί ανά game στο buffer
 
    // gameName -> RandomBuffer (producer-consumer buffer)
    static final Map<String, RandomBuffer> buffers = new HashMap<>();
    // gameName -> shared secret
    static final Map<String, String> secrets = new HashMap<>();
 
    public static void main(String[] args) throws IOException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
 
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("[SRGServer] Listening on port " + port);
 
        while (true) {
            Socket workerSocket = serverSocket.accept();
            new Thread(new SRGHandler(workerSocket)).start();
        }
    }

    static synchronized void registerGame(String gameName, String secret) {
        if (!buffers.containsKey(gameName)) {
            RandomBuffer buf = new RandomBuffer(BUFFER_SIZE);
            buffers.put(gameName, buf);
            secrets.put(gameName, secret);
 
            // Ξεκίνα producer thread για αυτό το παιχνίδι
            Thread producer = new Thread(new ProducerThread(buf), "Producer-" + gameName);
            producer.setDaemon(true);
            producer.start();
            System.out.println("[SRGServer] Registered game: " + gameName);
        }
    }
}

class RandomBuffer {
 
    private final int capacity;
    private final Queue<Integer> queue = new LinkedList<>();
 
    public RandomBuffer(int capacity) {
        this.capacity = capacity;
    }
 
    public synchronized void produce(int number) throws InterruptedException {
        while (queue.size() >= capacity) {
            wait();
        }
        queue.offer(number);
        notifyAll();
    }
 

    public synchronized int consume() throws InterruptedException {
        while (queue.isEmpty()) {
            wait(); 
        }
        int number = queue.poll();
        notifyAll();
        return number;
    }
 
    public synchronized int size() {
        return queue.size();
    }
}

class ProducerThread implements Runnable {
 
    private final RandomBuffer buffer;
    private final SecureRandom rng = new SecureRandom();
 
    private static final long PRODUCE_DELAY_MS = 50;
 
    public ProducerThread(RandomBuffer buffer) {
        this.buffer = buffer;
    }
 
    @Override
    public void run() {
        while (true) {
            try {
                int number = Math.abs(rng.nextInt()); // θετικός τυχαίος αριθμός
                buffer.produce(number);
                Thread.sleep(PRODUCE_DELAY_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
class SRGHandler implements Runnable {
 
    private final Socket socket;
 
    public SRGHandler(Socket socket) {
        this.socket = socket;
    }
 
    @Override
    public void run() {
        try (
            BufferedReader in  = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter    out = new PrintWriter(socket.getOutputStream(), true)
        ) {
            String line;
            while ((line = in.readLine()) != null) {
                String response = dispatch(line);
                out.println(response);
            }
        } catch (IOException e) {
            System.err.println("[SRGHandler] " + e.getMessage());
        } finally {
            try { socket.close(); } catch (IOException ignored) {}
        }
    }
 
    private String dispatch(String msg) {
        String[] p = msg.split("\\|");
        switch (p[0]) {
            case "REGISTER":   return handleRegister(p);
            case "GET_RANDOM": return handleGetRandom(p);
            default:           return "ERROR|Unknown command";
        }
    }
 
    // --- REGISTER|<gameName>|<secret> ---
    private String handleRegister(String[] p) {
        if (p.length < 3) return "ERROR|Missing gameName or secret";
        SRGServer.registerGame(p[1], p[2]);
        return "OK|Registered " + p[1];
    }
 
    // --- GET_RANDOM|<gameName> ---
    private String handleGetRandom(String[] p) {
        if (p.length < 2) return "ERROR|Missing gameName";
        String gameName = p[1];
 
        RandomBuffer buf = SRGServer.buffers.get(gameName);
        String secret    = SRGServer.secrets.get(gameName);
 
        if (buf == null || secret == null) {
            return "ERROR|Game not registered: " + gameName;
        }
 
        try {
            // Consumer: παίρνει αριθμό από buffer (wait αν άδειο)
            int number = buf.consume();
 
            // Υπολογισμός sha256(number + secret) για επαλήθευση από Worker
            String hash = sha256(number + secret);
 
            return number + "|" + hash;
 
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "ERROR|Interrupted";
        }
    }
 
    private String sha256(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(input.getBytes("UTF-8"));
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException("SHA-256 failed", e);
        }
    }
}