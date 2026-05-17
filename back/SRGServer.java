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

    // Stores the secret (HashKey) for each registered game.
    private static final Map<String, String> gameSecrets = new HashMap<>();

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

    /**
     * Registers a game: stores its secret and creates/starts its producer buffer.
     * Called when ADD_GAME is received (sent by the Worker after a game is added).
     */
    static void registerGame(String gameName, String secret) {
        synchronized (gameBuffers) {
            // Store secret regardless (allow re-registration / update)
            gameSecrets.put(gameName, secret);

            if (!gameBuffers.containsKey(gameName)) {
                GameBuffer gameBuffer = new GameBuffer(gameName);
                gameBuffers.put(gameName, gameBuffer);
                new Thread(new Producer(gameBuffer)).start();
                System.out.println("[SRG] Registered game=" + gameName + " and started producer.");
            } else {
                System.out.println("[SRG] Updated secret for game=" + gameName);
            }
        }
    }

    /**
     * Returns the buffer for a game that was already registered via ADD_GAME.
     * Returns null if the game has not been registered.
     */
    private static GameBuffer getGameBuffer(String gameName) {
        synchronized (gameBuffers) {
            return gameBuffers.get(gameName);
        }
    }

    private static String getSecret(String gameName) {
        synchronized (gameBuffers) {
            return gameSecrets.get(gameName);
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

    // Continuously generates random numbers and stores them in the shared buffer.
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
                    gameBuffer.lock.notifyAll();
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

    // Handles incoming requests from workers.
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
            String cmd = parts[0];

            return switch (cmd) {
                case "ADD_GAME"   -> handleAddGame(parts);
                case "GET_RANDOM" -> handleGetRandom(parts);
                default           -> "ERROR|Unknown command: " + cmd;
            };
        }

        /**
         * ADD_GAME|gameName|secret
         *
         * Registers the game with its secret and starts a producer for it.
         * Must be called by the Worker every time a new game is added.
         */
        private String handleAddGame(String[] parts) {
            if (parts.length < 3) {
                return "ERROR|Usage: ADD_GAME|gameName|secret";
            }
            String gameName = parts[1].trim();
            String secret   = parts[2].trim();

            if (gameName.isEmpty()) {
                return "ERROR|Missing game name";
            }
            if (secret.isEmpty()) {
                return "ERROR|Missing secret";
            }

            registerGame(gameName, secret);
            return "OK|GAME_REGISTERED|" + gameName;
        }

        /**
         * GET_RANDOM|gameName
         *
         * Returns a random number from the game's producer buffer together with
         * sha256(number + storedSecret) so the Worker can verify integrity.
         * The secret is NOT sent in the request — it was stored at ADD_GAME time.
         */
        private String handleGetRandom(String[] parts) {
            if (parts.length < 2) {
                return "ERROR|Usage: GET_RANDOM|gameName";
            }
            String gameName = parts[1].trim();

            if (gameName.isEmpty()) {
                return "ERROR|Missing game name";
            }

            GameBuffer gameBuffer = getGameBuffer(gameName);
            if (gameBuffer == null) {
                return "ERROR|Game not registered: " + gameName;
            }

            String secret = getSecret(gameName);
            if (secret == null) {
                return "ERROR|No secret for game: " + gameName;
            }

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
                number = gameBuffer.buffer.poll();
                gameBuffer.lock.notifyAll();
            }

            String hash = sha256(number + secret);
            System.out.println("[SRG] Sent to game=" + gameName + " number=" + number);
            return number + "|" + hash;
        }

        // Computes SHA-256 hash used to verify randomness integrity.
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