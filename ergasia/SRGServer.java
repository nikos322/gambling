import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;

public class SRGServer {

    private static final Queue<Integer> buffer = new LinkedList<>();
    private static final Object lock = new Object();
    private static final int MAX = 10;

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("Usage: java SRGServer <port>");
            return;
        }

        int port = Integer.parseInt(args[0]);

        new Thread(new Producer()).start();

        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("[SRG] Running on port " + port);

        while (true) {
            Socket socket = serverSocket.accept();
            new Thread(new Handler(socket)).start();
        }
    }
    
    static class Producer implements Runnable {
        @Override
        public void run() {
            Random rand = new Random();

            while (true) {
                synchronized (lock) {
                    while (buffer.size() >= MAX) {
                        try {
                            lock.wait();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }

                    int num = rand.nextInt(Integer.MAX_VALUE);
                    buffer.add(num);
                    System.out.println("[SRG] Produced: " + num);

                    lock.notifyAll();
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

            if (secret == null || secret.isBlank()) {
                return "ERROR|Missing secret";
            }

            int number;
            synchronized (lock) {
                while (buffer.isEmpty()) {
                    try {
                        lock.wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return "ERROR|Interrupted";
                    }
                }

                number = buffer.poll();
                lock.notifyAll();
            }

            String hash = sha256(String.valueOf(number) + secret);
            System.out.println("[SRG] Sent to game=" + gameName + " number=" + number);

            return number + "|" + hash;
        }

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