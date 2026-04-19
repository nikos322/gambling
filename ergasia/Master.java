import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

// Master node that coordinates communication between clients, workers and reducer.
public class Master {
    private static final int MASTER_PORT = 5000;

    private static final List<WorkerConnection> workerConnections = new ArrayList<>();
    private static ReducerConnection reducerConnection;
    private static String reducerHost;
    private static int reducerPort;
    private static int numOfWorkers;
    //TODO after init import all games from json / make json
    // Initializes connections to workers and reducer.
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Usage: java Master <reducerHost:port> <worker1:port> <worker2:port> ...");
            return;
        }

        String[] reducerAddr = args[0].split(":");
        if (reducerAddr.length != 2) {
            System.out.println("ERROR: Invalid reducer address. Expected host:port");
            return;
        }

        reducerHost = reducerAddr[0];
        reducerPort = Integer.parseInt(reducerAddr[1]);

        reducerConnection = new ReducerConnection(reducerHost, reducerPort);
        reducerConnection.connect();

        initWorkerConnections(Arrays.copyOfRange(args, 1, args.length));
        numOfWorkers = workerConnections.size();

        if (numOfWorkers == 0) {
            System.out.println("[Master] No workers configured.");
            return;
        }

        ServerSocket serverSocket = new ServerSocket(MASTER_PORT);
        System.out.println("[Master] Listening on port " + MASTER_PORT);
        System.out.println("[Master] Connected to reducer at " + reducerHost + ":" + reducerPort);
        System.out.println("[Master] Connected to " + numOfWorkers + " workers.");

        while (true) {
            Socket clientSocket = serverSocket.accept();
            System.out.println("[Master] New client: " + clientSocket.getInetAddress());
            new Thread(new ClientHandler(clientSocket)).start();
        }
    }

    // Establishes connections with all worker nodes.
    private static void initWorkerConnections(String[] workerAddresses) {
        for (String address : workerAddresses) {
            String[] parts = address.split(":");
            if (parts.length != 2) {
                System.out.println("[Master] Invalid worker address skipped: " + address);
                continue;
            }
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
            WorkerConnection wc = new WorkerConnection(host, port);
            wc.connect();
            workerConnections.add(wc);
            System.out.println("[Master] Connected to worker at " + address);
        }
    }

    public static int routeToWorker(String gameName) {
        return Math.abs(gameName.hashCode()) % numOfWorkers;
        // Selects a worker based on game name using hashing.
    }
    public static WorkerConnection getWorkerConnection(int workerId) {
        return workerConnections.get(workerId);
    }
    public static List<WorkerConnection> getAllWorkerConnections() {
        return workerConnections;
    }
    public static ReducerConnection getReducerConnection() {
        return reducerConnection;
    }
    public static String getReducerHost() {
        return reducerHost;
    }
    public static int getReducerPort() {
        return reducerPort;
    }
    public static int getNumberOfWorkers() {
        return numOfWorkers;
    }
}

class ClientHandler implements Runnable {
    private final Socket socket;

    private static final String CMD_ADD_GAME = "ADD_GAME";
    private static final String CMD_REMOVE_GAME = "REMOVE_GAME";
    private static final String CMD_EDIT_GAME = "EDIT_GAME";
    private static final String CMD_SEARCH = "SEARCH";
    private static final String CMD_PLAY = "PLAY";
    private static final String CMD_ADD_BALANCE = "ADD_BALANCE";
    private static final String CMD_STATS_PROVIDER = "STATS_PROVIDER";
    private static final String CMD_STATS_PLAYER = "STATS_PLAYER";

    public ClientHandler(Socket socket) {
        this.socket = socket;
    }

    @Override
    // Reads client requests and sends back responses.
    public void run() {
        try (
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
        ) {
            String line;
            while ((line = in.readLine()) != null) {
                System.out.println("[ClientHandler] Received: " + line);
                String response = handleCommand(line);
                out.println(response);
            }
        } catch (IOException e) {
            System.err.println("[ClientHandler] Connection error: " + e.getMessage());
        } finally {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
    }

    // Parses client command and routes it to the appropriate handler.
    private String handleCommand(String rawMessage) {
        if (rawMessage == null || rawMessage.isBlank()) {
            return "ERROR|Empty command";
        }

        String[] parts2 = rawMessage.split("\\|", 2);
        String cmd = parts2[0];

        return switch (cmd) {
            case CMD_ADD_GAME -> handleAddGame(parts2);
            case CMD_REMOVE_GAME -> handleRemoveGame(parts2);
            case CMD_EDIT_GAME -> handleEditGame(rawMessage.split("\\|"));
            case CMD_SEARCH -> handleSearch(rawMessage.split("\\|"));
            case CMD_PLAY -> handlePlay(rawMessage.split("\\|"));
            case CMD_ADD_BALANCE -> handleAddBalance(rawMessage.split("\\|"));
            case CMD_STATS_PROVIDER -> handleStatsProvider(rawMessage.split("\\|"));
            case CMD_STATS_PLAYER -> handleStatsPlayer(rawMessage.split("\\|"));
            default -> "ERROR|Unknown command: " + cmd;
        };
    }

    private String handleAddGame(String[] parts) {
        String gameJson = parts.length > 1 ? parts[1] : "";
        String gameName = parseGameName(gameJson);

        if (gameName.isEmpty()) {
            return "ERROR|GameName not found in JSON";
        }

        int workerId = Master.routeToWorker(gameName);
        return Master.getWorkerConnection(workerId).sendAndReceive("ADD_GAME|" + gameJson);
    }

    private String handleRemoveGame(String[] parts) {
        String gameName = parts.length > 1 ? parts[1] : "";

        if (gameName.isEmpty()) {
            return "ERROR|Missing game name";
        }

        int workerId = Master.routeToWorker(gameName);
        return Master.getWorkerConnection(workerId).sendAndReceive("REMOVE_GAME|" + gameName);
    }

    private String handleEditGame(String[] p) {
        if (p.length < 4) {
            return "ERROR|Usage: EDIT_GAME|gameName|field|value";
        }

        String gameName = p[1];
        int workerId = Master.routeToWorker(gameName);

        return Master.getWorkerConnection(workerId)
                .sendAndReceive("EDIT_GAME|" + p[1] + "|" + p[2] + "|" + p[3]);
    }

    private String handleSearch(String[] p) {
        if (p.length < 4) {
            return "ERROR|Usage: SEARCH|risk|betCategory|minStars";
        }
        String risk = p[1];
        String bet = p[2];
        String minStars = p[3];
        String jobId = "job-search-" + System.currentTimeMillis();
        int n = Master.getNumberOfWorkers();

        //Initializes reducer
        String initAck = Master.getReducerConnection()
                .sendAndReceive("INIT_REDUCE_SEARCH|" + jobId + "|" + n);
        System.out.println("[Master] Reducer init search: " + initAck);

        //Sends search request to all workers
        List<Thread> threads = new ArrayList<>();
        for (WorkerConnection wc : Master.getAllWorkerConnections()) {
            String msg = "MAP_SEARCH|" + jobId + "|" + risk + "|" + bet + "|" + minStars
                    + "|" + Master.getReducerHost() + "|" + Master.getReducerPort();

            Thread t = new Thread(() -> wc.sendAndReceive(msg));
            threads.add(t);
            t.start();
        }

        joinAll(threads);

        return Master.getReducerConnection().sendAndReceive("GET_RESULT|" + jobId);
    }

    // Sends play request to the worker responsible for the specific game.
    private String handlePlay(String[] p) {
        if (p.length < 4) {
            return "ERROR|Usage: PLAY|playerId|gameName|betAmount";
        }
        String playerId = p[1];
        String gameName = p[2];
        String betAmount = p[3];

        int workerId = Master.routeToWorker(gameName);
        return Master.getWorkerConnection(workerId)
                    .sendAndReceive("PLAY|" + playerId + "|" + gameName + "|" + betAmount);
    }

    // Sends balance update to all workers and returns the first successful response.
    private String handleAddBalance(String[] p) {
        if (p.length < 3) {
            return "ERROR|Usage: ADD_BALANCE|playerId|amount";
        }

        String playerId = p[1];
        String amount = p[2];

        List<String> responses = Collections.synchronizedList(new ArrayList<>());
        List<Thread> threads = new ArrayList<>();

        for (WorkerConnection wc : Master.getAllWorkerConnections()) {
            Thread t = new Thread(() -> {
                String res = wc.sendAndReceive("ADD_BALANCE|" + playerId + "|" + amount);
                responses.add(res);
            });
            threads.add(t);
            t.start();
        }

        joinAll(threads);

        for (String r : responses) {
            if (r != null && r.startsWith("OK|")) {
                return r;
            }
        }

        return "ERROR|Balance update failed";
    }

    //Retrieves provider statistics using MapReduce: workers send partial data which the reducer aggregates.//
    private String handleStatsProvider(String[] p) {
        if (p.length < 2) {
            return "ERROR|Usage: STATS_PROVIDER|providerName";
        }

        String providerName = p[1];
        String jobId = "job-prov-" + System.currentTimeMillis();
        int n = Master.getNumberOfWorkers();

        String initAck = Master.getReducerConnection()
                .sendAndReceive("INIT_REDUCE_PROVIDER|" + jobId + "|" + n);
        System.out.println("[Master] Reducer init: " + initAck);

        List<Thread> threads = new ArrayList<>();
        for (WorkerConnection wc : Master.getAllWorkerConnections()) {
            String msg = "MAP_PROVIDER|" + jobId + "|" + providerName
                    + "|" + Master.getReducerHost() + "|" + Master.getReducerPort();

            Thread t = new Thread(() -> wc.sendAndReceive(msg));
            threads.add(t);
            t.start();
        }

        joinAll(threads);
        return Master.getReducerConnection().sendAndReceive("GET_RESULT|" + jobId);
    }

    private String handleStatsPlayer(String[] p) {
        if (p.length < 2) {
            return "ERROR|Usage: STATS_PLAYER|playerId";
        }

        String playerId = p[1];
        String jobId = "job-player-" + System.currentTimeMillis();
        int n = Master.getNumberOfWorkers();

        String initAck = Master.getReducerConnection()
                .sendAndReceive("INIT_REDUCE_PLAYER|" + jobId + "|" + n);
        System.out.println("[Master] Reducer init: " + initAck);

        List<Thread> threads = new ArrayList<>();
        for (WorkerConnection wc : Master.getAllWorkerConnections()) {
            String msg = "MAP_PLAYER|" + jobId + "|" + playerId
                    + "|" + Master.getReducerHost() + "|" + Master.getReducerPort();

            Thread t = new Thread(() -> wc.sendAndReceive(msg));
            threads.add(t);
            t.start();
        }

        joinAll(threads);
        return Master.getReducerConnection().sendAndReceive("GET_RESULT|" + jobId);
    }

    //Wait for all Worker threads to finish to continue
    private void joinAll(List<Thread> threads) {
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("[Master] Thread interrupted");
            }
        }
    }

    private String parseGameName(String gameJson) {
        if (gameJson == null) return "";

        String key = "\"GameName\"";
        int keyPos = gameJson.indexOf(key);
        if (keyPos < 0) return "";

        int colonPos = gameJson.indexOf(':', keyPos);
        if (colonPos < 0) return "";

        int firstQuote = gameJson.indexOf('"', colonPos + 1);
        if (firstQuote < 0) return "";

        int secondQuote = gameJson.indexOf('"', firstQuote + 1);
        if (secondQuote < 0) return "";

        return gameJson.substring(firstQuote + 1, secondQuote).trim();
    }
}

// Manages TCP communication with a worker node.
class WorkerConnection {
    private final String host;
    private final int port;

    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;

    public WorkerConnection(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() {
        try {
            socket = new Socket(host, port);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        } catch (IOException e) {
            System.err.println("[WorkerConnection] Cannot connect to " + host + ":" + port);
        }
    }

    // Sends a request to a worker and waits for response.
    public synchronized String sendAndReceive(String message) {
        try {
            out.println(message);
            String response = in.readLine();
            return response == null ? "ERROR|Worker closed connection" : response;
        } catch (IOException e) {
            System.err.println("[WorkerConnection] Error: " + e.getMessage());
            return "ERROR|Worker communication failure";
        }
    }

    public void close() {
        try {
            if (socket != null) socket.close();
        } catch (IOException ignored) {
        }
    }
}

// Manages TCP communication with the reducer node.
class ReducerConnection {
    private final String host;
    private final int port;

    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;

    public ReducerConnection(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() {
        try {
            socket = new Socket(host, port);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        } catch (IOException e) {
            System.err.println("[ReducerConnection] Cannot connect to " + host + ":" + port);
        }
    }

    // Sends a request to the reducer and receives aggregated results.
    public synchronized String sendAndReceive(String message) {
        try {
            out.println(message);
            String response = in.readLine();
            return response == null ? "ERROR|Reducer closed connection" : response;
        } catch (IOException e) {
            System.err.println("[ReducerConnection] Error: " + e.getMessage());
            return "ERROR|Reducer communication failure";
        }
    }

    public void close() {
        try {
            if (socket != null) socket.close();
        } catch (IOException ignored) {
        }
    }
}