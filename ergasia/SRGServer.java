import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Random;

// Generates secure random numbers for games and ensures integrity using hashing.
public class SRGServer {
    private static final int MAX = 10;

    // Stores one random-number buffer per game.
    private static final Map<String, GameBuffer> gameBuffers = new HashMap<>();

    // Starts the SRG server. A producer is created lazily for each game when it is first played.
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("Usage: java SRGServer <port>");
            return;
        }
        int port = Integer.parseInt(args[0]);

        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("[SRG] Running on port " + port);

        while (true) {
            Socket socket = serverSocket.accept();
            new Thread(new Handler(socket)).start();
        }
    }

    // Returns the buffer of a game. If the game is played for the first time, it creates a new producer.
    private static GameBuffer getOrCreateGameBuffer(String gameName) {
        synchronized (gameBuffers) {
            GameBuffer gameBuffer = gameBuffers.get(gameName);

            if (gameBuffer == null) {
                gameBuffer = new GameBuffer(gameName);
                gameBuffers.put(gameName, gameBuffer);

                new Thread(new Producer(gameBuffer)).start();
                System.out.println("[SRG] Created producer for game=" + gameName);
            }

            return gameBuffer;
        }
    }

    // Keeps the queue and lock of one specific game.
    static class GameBuffer {
        private final String gameName;
        private final Queue<Integer> buffer = new LinkedList<>();
        private final Object lock = new Object();

        GameBuffer(String gameName) {
            this.gameName = gameName;
        }
    }

    // Continuously generates random numbers and stores them in a shared buffer
    static class Producer implements Runnable {
        private final GameBuffer gameBuffer;

        Producer(GameBuffer gameBuffer) {
            this.gameBuffer = gameBuffer;
        }

        @Override
        public void run() {
            Random rand = new Random();

            while (true) {
                synchronized (gameBuffer.lock) {
                    while (gameBuffer.buffer.size() >= MAX) {
                        try {
                            gameBuffer.lock.wait();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                    int num = rand.nextInt(Integer.MAX_VALUE);
                    gameBuffer.buffer.add(num);
                    System.out.println("[SRG][" + gameBuffer.gameName + "] Produced: " + num);
                    gameBuffer.lock.notifyAll(); // Notifies waiting threads that buffer state has changed.
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    // Handles incoming requests from workers
    static class Handler implements Runnable {
        private final Socket socket;

        Handler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try (
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
            ) {
                String request;
                while ((request = in.readLine()) != null) {
                    String response = handleRequest(request);
                    out.println(response);
                }
            } catch (IOException e) {
                System.out.println("[SRG] Worker disconnected");
            } finally {
                try {
                    socket.close();
                } catch (IOException ignored) {
                }
            }
        }

        private String handleRequest(String request) {
            if (request == null || request.isBlank()) {
                return "ERROR|Empty request";
            }
            String[] parts = request.split("\\|");
            if (parts.length < 3) {
                return "ERROR|Usage: GET_RANDOM|gameName|secret";
            }
            String cmd = parts[0];
            String gameName = parts[1];
            String secret = parts[2];

            if (!"GET_RANDOM".equals(cmd)) {
                return "ERROR|Unknown command";
            }
            if (gameName == null || gameName.isBlank()) {
                return "ERROR|Missing game name";
            }
            if (secret == null || secret.isBlank()) {
                return "ERROR|Missing secret";
            }

            GameBuffer gameBuffer = getOrCreateGameBuffer(gameName);

            int number;
            synchronized (gameBuffer.lock) {
                while (gameBuffer.buffer.isEmpty()) {
                    try {
                        gameBuffer.lock.wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return "ERROR|Interrupted";
                    }
                }
                number = gameBuffer.buffer.poll(); //Retrieves and removes a random number from the buffer
                gameBuffer.lock.notifyAll();
            }

            String hash = sha256(String.valueOf(number) + secret);
            System.out.println("[SRG] Sent to game=" + gameName + " number=" + number);
            return number + "|" + hash; //Returns random number along with hash for verification
        }

        // Computes SHA-256 hash used to verify randomness integrity
        private String sha256(String value) {
            try {
                MessageDigest md = MessageDigest.getInstance("SHA-256");
                byte[] hash = md.digest(value.getBytes(StandardCharsets.UTF_8));
                StringBuilder sb = new StringBuilder();
                for (byte b : hash) {
                    sb.append(String.format("%02x", b));
                }
                return sb.toString();
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("SHA-256 unavailable", e);
            }
        }
    }
}