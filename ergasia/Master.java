import java.io.*;
import java.net.*;
import java.util.*;

public class Master {

    private static final int MASTER_PORT = 5000;
    private static int numOfWorkers;
    private static List<WorkerConnection> workerConnections = new ArrayList<>();
    private static ReducerConnection reducerConnection;

    private static String reducerHost;
    private static int reducerPort;

    public static void main(String[] args) throws IOException{
        
        if (args.length == 0) {
            System.out.println("Usage: Master <reducerHost:port> <worker1:port> <worker2:port> ...");
            return;
        }
        String[] reducerAddr = args[0].split(":");
        reducerHost = reducerAddr[0];
        reducerPort = Integer.parseInt(reducerAddr[1]);
        reducerConnection = new ReducerConnection(reducerHost,reducerPort);
        reducerConnection.connect();
        numOfWorkers = args.length;
        initWorkerConnections(args);

        ServerSocket serverSocket = new ServerSocket(MASTER_PORT);
        System.out.println("[Master] Listening on port " + MASTER_PORT);
        System.out.println("[Master] Connected to " + numOfWorkers + " workers.");

        while (true){
            Socket clientSocket = serverSocket.accept();
            System.out.println("[Master] New client: " + clientSocket.getInetAddress());
            Thread handler = new Thread(new ClientHandler(clientSocket));
            handler.start();
        }

    }

    
    private static void initWorkerConnections(String[] workerAddresses){
        for (String address : workerAddresses) {
            String[] parts = address.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
            WorkerConnection wc = new WorkerConnection(host, port);
            wc.connect();
            workerConnections.add(wc);
            System.out.println("[Master] Connected to worker at " + address);

        }
    }

    public static int routeToWorker(String gameName){
        return Math.abs(gameName.hashCode()) % numOfWorkers;
    }

    public static WorkerConnection getWorkerConnection(int workerId){
        return workerConnections.get(workerId);
    }

    public static List<WorkerConnection> getAllWorkerConnections(){
        return workerConnections;
    }
    static ReducerConnection getReducerConnection(){
        return reducerConnection;
    }
    static String getReducerHost() { return reducerHost; }
    static int    getReducerPort() { return reducerPort; }
    static int    getNumberOfWorkers() { return numOfWorkers; }
}

class ClientHandler implements Runnable{
    private final Socket socket;

    private static final String CMD_ADD_GAME    = "ADD_GAME";
    private static final String CMD_REMOVE_GAME = "REMOVE_GAME";
    private static final String CMD_EDIT_GAME   = "EDIT_GAME";
    private static final String CMD_SEARCH      = "SEARCH";
    private static final String CMD_PLAY        = "PLAY";
    private static final String CMD_ADD_BALANCE = "ADD_BALANCE";
    private static final String CMD_STATS_PROV  = "STATS_PROVIDER";
    private static final String CMD_STATS_PLAYER= "STATS_PLAYER";

    public ClientHandler(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        try (
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter   out = new PrintWriter(socket.getOutputStream(),true)
        ) {
            String line;
            while ((line = in.readLine()) != null){
                System.out.println("[ClientHandler] Received: " + line);
                String response = handleCommand(line);
                out.println(response);
            }
        } catch (IOException e) {
            System.err.println("[ClientHandler] Connection error: " + e.getMessage());
        } finally {
            try {socket.close();} catch (IOException ignored) {}
        }
    }

    private String handleCommand(String rawMessage) {
        String[] parts = rawMessage.split("\\|", 2);
        String cmd = parts[0];
        return switch (cmd) {
            case CMD_ADD_GAME -> handleAddGame(parts);
            case CMD_REMOVE_GAME -> handleRemoveGame(parts);
            case CMD_EDIT_GAME -> handleEditGame(rawMessage.split("\\|"));
            case CMD_SEARCH -> handleSearch(rawMessage.split("\\|"));
            case CMD_PLAY -> handlePlay(rawMessage.split("\\|"));
            case CMD_ADD_BALANCE -> handleAddBalance(rawMessage.split("\\|"));
            case CMD_STATS_PROV -> handleStatsProvider(rawMessage.split("\\|"));
            case CMD_STATS_PLAYER -> handleStatsPlayer(rawMessage.split("\\|"));
            default -> "ERROR|Unknown command: " + cmd;
        };
    }
 
    // --- ADD_GAME|<json> ---
    private String handleAddGame(String[] parts) {
        String gameJson = parts.length > 1 ? parts[1] : "";
        String gameName = parseGameName(gameJson);
        int workerId = Master.routeToWorker(gameName);
        return Master.getWorkerConnection(workerId).sendAndReceive("ADD_GAME|" + gameJson);
    }
 
    // --- REMOVE_GAME|<gameName> ---
    private String handleRemoveGame(String[] parts) {
        String gameName = parts.length > 1 ? parts[1] : "";
        int workerId = Master.routeToWorker(gameName);
        return Master.getWorkerConnection(workerId).sendAndReceive("REMOVE_GAME|" + gameName);
    }
 
    // --- EDIT_GAME|<gameName>|<field>|<value> ---
    private String handleEditGame(String[] p) {
        if (p.length < 4) return "ERROR|Invalid edit command";
        int workerId = Master.routeToWorker(p[1]);
        return Master.getWorkerConnection(workerId)
                .sendAndReceive("EDIT_GAME|" + p[1] + "|" + p[2] + "|" + p[3]);
    }
 
    // --- SEARCH|<riskLevel>|<betCategory>|<minStars> ---
    // Fan-out σε όλους τους Workers παράλληλα
    private String handleSearch(String[] p) {
        String filters = p.length > 1 ? String.join("|", Arrays.copyOfRange(p, 1, p.length)) : "";
        List<String> partialResults = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
 
        for (WorkerConnection wc : Master.getAllWorkerConnections()) {
            final String msg = "SEARCH|" + filters;
            Thread t = new Thread(() -> {
                String result = wc.sendAndReceive(msg);
                synchronized (partialResults) { partialResults.add(result); }
            });
            threads.add(t);
            t.start();
        }
        joinAll(threads);
 
        StringBuilder merged = new StringBuilder("SEARCH_RESULTS|");
        for (String r : partialResults) {
            if (!r.equals("EMPTY")) merged.append(r);
        }
        return merged.toString();
    }
 
    // --- PLAY|<playerId>|<gameName>|<betAmount> ---
    private String handlePlay(String[] p) {
        if (p.length < 4) return "ERROR|Invalid play command";
        int workerId = Master.routeToWorker(p[2]);
        return Master.getWorkerConnection(workerId)
                .sendAndReceive("PLAY|" + p[1] + "|" + p[2] + "|" + p[3]);
    }
 
    // --- ADD_BALANCE|<playerId>|<amount> ---
    private String handleAddBalance(String[] p) {
        // TODO: υλοποιήστε διαχείριση υπολοίπου
        return "OK|Balance updated";
    }
 
    // --- STATS_PROVIDER|<providerName> ---
    // Νέος MapReduce flow: Master → Reducer (INIT) → Workers (MAP) → Reducer (collect) → Master (GET_RESULT)
    private String handleStatsProvider(String[] p) {
        String providerName = p.length > 1 ? p[1] : "";
        String jobId = "job-prov-" + System.currentTimeMillis();
        int n = Master.getNumberOfWorkers();
 
        // Βήμα 1: Ειδοποίησε τον Reducer να περιμένει N αποτελέσματα
        String initAck = Master.getReducerConnection()
                .sendAndReceive("INIT_REDUCE_PROVIDER|" + jobId + "|" + n);
        System.out.println("[Master] Reducer init: " + initAck);
 
        // Βήμα 2: Στείλε MAP εντολή σε κάθε Worker παράλληλα
        // Ο κάθε Worker θα στείλει αποτέλεσμα ΑΠΕΥΘΕΙΑΣ στον Reducer
        List<Thread> threads = new ArrayList<>();
        for (WorkerConnection wc : Master.getAllWorkerConnections()) {
            String msg = "MAP_PROVIDER|" + jobId + "|" + providerName
                    + "|" + Master.getReducerHost() + "|" + Master.getReducerPort();
            Thread t = new Thread(() -> wc.sendAndReceive(msg));
            threads.add(t);
            t.start();
        }
        joinAll(threads);
 
        // Βήμα 3: Ζήτα το τελικό αποτέλεσμα από τον Reducer (blocking)
        return Master.getReducerConnection().sendAndReceive("GET_RESULT|" + jobId);
    }
 
    // --- STATS_PLAYER|<playerId> ---
    private String handleStatsPlayer(String[] p) {
        String playerId = p.length > 1 ? p[1] : "";
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
 
    // -----------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------
    private void joinAll(List<Thread> threads) {
        for (Thread t : threads) {
            try { t.join(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }
 
    
}

class WorkerConnection {

    private final String host;
    private final int port;
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;

    public WorkerConnection(String host, int port) {
        this.port = port;
        this.host = host;
    }

    public void connect(){
        try {
            socket = new Socket(host,port);
            out = new PrintWriter(socket.getOutputStream(),true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        } catch (IOException e) {
            System.err.println("[WorkerConnection] Cannot connect to "+ host + ":"+ port);
        }
    }

    public synchronized String sendAndReceive(String Message){
        try {
            out.println(Message);
            return in.readLine();
        } catch (IOException e) {
            System.err.println("[WorkerConnection] Error: "+ e.getMessage());
            return "ERROR";
        }
    }

    public void close(){
        try{ if (socket != null) socket.close();} catch (IOException ignored) {}
    }
}

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

    public void connect(){
        try {
            socket = new Socket(host,port);
            out = new PrintWriter(socket.getOutputStream(),true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        } catch (IOException e) {
            System.err.println("[ReducerConnection] Cannot connect to "+ host + ":"+ port);
        }
    }

    public synchronized  String sendAndReceive(String Message){
        try {
            out.println(Message);
            return in.readLine();
        } catch (IOException e) {
            System.err.println("[ReducerConnection] Error: "+ e.getMessage());
            return "ERROR";
        }
    }
    public void close(){
        try{ if (socket != null) socket.close();} catch (IOException ignored) {}
    }
}