import java.io.*;
import java.net.*;
import java.security.MessageDigest;
import java.util.*;

public class Worker {
    private static final int DEFAULT_PORT = 5001;
    private static int workerPort;

    // gameName -> GameInfo
    static Map<String,GameInfo> games = new HashMap<>();
    // gamename -> system profit
    static Map<String,Double> gameStats = new HashMap<>();
    // playerId -> player profit
    static Map<String,Double> playerStats = new HashMap<>();

    private static String srgHost;
    private static int srgPort;

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.err.println("Usage:  Worker <port> <srgHost> <srgPort>");
        }

        workerPort = Integer.parseInt(args[0]);
        srgHost = args[1];
        srgPort = Integer.parseInt(args[2]);

        ServerSocket serverSocket = new ServerSocket(workerPort);
        System.out.println("[Worker] Listening on port: "+ workerPort);

        while (true) { 
            Socket masterSocket = serverSocket.accept();
            new Thread(new WorkerHandler(masterSocket,srgHost,srgPort)).start();
        }
    }

    public static String getSrgHost() { 
        return srgHost;
    }

    public static int getSrgPort() { 
        return srgPort;
    }
}

class GameInfo{
    
}