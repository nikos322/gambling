import java.io.*;
import java.net.*;
import java.util.*;

public class Master {

    private static final int MASTER_PORT = 5000;
    private static int numOfWorkers;
    private static List<WorkerConnection> workerConnections = new ArrayList<>();

    public static void main(String[] args) throws IOException{
        
        if (args.length == 0) {
            System.out.println("Usage: Master <worker1:port> <worker2:port> ...");
            return;
        }

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
            System.err.println("[Clienthandler] Connection error: " + e.getMessage());
        } finally {
            try {socket.close();} catch (IOException ignored) {}
        }
    }

    private String handleCommand(String rawMessage){
        //todo 
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

    public synchronized String sendAndRecieve(String Message){
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

    public String sendAndRecieve(String Message){
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