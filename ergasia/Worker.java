import java.io.*;
import java.net.*;
import java.security.MessageDigest;
import java.util.*;

public class Worker {
    private static final int DEFAULT_PORT = 5001;
    private static int workerPort;

    // gameName -> GameInfo
    static final Map<String,GameInfo> games = new HashMap<>();
    // gamename -> system profit
    static final Map<String,Double> gameStats = new HashMap<>();
    // playerId -> player profit
    static final Map<String,Double> playerStats = new HashMap<>();

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
    String gameName;
    String providerName;
    double stars;
    int    noOfVotes;
    String gameLogo;
    double minBet;
    double maxBet;
    String riskLevel;
    String betCategory;
    double jackpot;
    String hashKey;
    boolean active = true;

    static final double[] LOW_MULT    = {0.0,0.0,0.0,0.1,0.5,1.0,1.1,1.3,2.0,2.5};
    static final double[] MEDIUM_MULT = {0.0,0.0,0.0,0.0,0.0,0.5,1.0,1.5,2.5,3.5};
    static final double[] HIGH_MULT   = {0.0,0.0,0.0,0.0,0.0,0.0,0.0,1.0,2.0,6.5};
 
    static final double LOW_JACKPOT    = 10;
    static final double MEDIUM_JACKPOT = 20;
    static final double HIGH_JACKPOT   = 40;

    void computeDerivedFields(){
        if (minBet < 1.0) {
            betCategory = "$";
        } else if (minBet < 5.0) {
            betCategory = "$$";
        } else {
            betCategory = "$$$";
        }

        jackpot = switch (riskLevel.toLowerCase()) {
            case "medium" -> MEDIUM_JACKPOT;
            case "high" -> HIGH_JACKPOT;
            default -> LOW_JACKPOT;
        };
    }

    double[] getMultipliers(){
        return switch (riskLevel.toLowerCase()) {
            case "medium" -> MEDIUM_MULT;
            case "high" -> HIGH_MULT;
            default -> LOW_MULT;
        };
    }

    boolean matchesFilter(String riskFilter, String betFilter, double minStars) {
        if (!active) return false;
        if (riskFilter != null && !riskFilter.isEmpty() && !riskLevel.equalsIgnoreCase(riskFilter)) return false;
        if (betFilter != null && !betFilter.isEmpty() && !betCategory.equalsIgnoreCase(betFilter)) return false;
        if (stars < minStars) return false;
        return true;
    }

    @Override
    public String toString() {
        return gameName + "," + providerName + "," + stars + "," + minBet + "," + maxBet
                + "," + riskLevel + "," + betCategory + "," + jackpot;
    }
}

class WorkerHandler implements Runnable{
    private final Socket socket;
    private final String srgHost;
    private final int srgPort;

    public WorkerHandler(Socket socket, String srgHost, int srgPort) {
        this.socket = socket;
        this.srgHost = srgHost;
        this.srgPort = srgPort;
    }

    @Override
    public void run() {
        try (
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        ){
            String line;
            while ((line = in.readLine()) != null) {
                String response = connectAction(line);
                out.println(response);
            }
        } catch (IOException e) {
            System.err.println("[WorkerHandler] "+ e.getMessage());
        } finally {
            try {socket.close();} catch (IOException ignored) {}
        }
    }


    private String connectAction(String message){
        String[] p = message.split("\\|");
        return switch (p[0]) {
            case "ADD_GAME" -> addGame(p);
            case "REMOVE_GAME" -> removeGame(p);
            case "EDIT_GAME" -> editGame(p);
            case "SEARCH" -> search(p);
            case "PLAY" -> play(p);
            case "MAP_STATS_PROVIDER" -> mapStatsProvider(p);
            case "MAP_STATS_PLAYER" -> mapStatsPlayer(p);
            default -> "ERROR|Unknown command";
        };
    }
    
    private String addGame(String[] m){
        if (m.length < 2) return "ERROR|No game data";
        GameInfo game = parseGameJson(m[1]);
        if (game == null) return "ERROR|Invalid Json";

        game.computeDerivedFields();

        synchronized (Worker.games) {
            Worker.games.put(game.gameName, game);
            Worker.gameStats.putIfAbsent(game.gameName, 0.0);
        }
        System.out.println("[Worker] Added game: " + game.gameName);
        return "OK|Game added: " + game.gameName;
    }

    private String removeGame(String[] m){
        String name = m.length > 1 ? m[1] :"";
        synchronized (Worker.games) {
            GameInfo g = Worker.games.get(name);
            if (g == null) return "ERROR|Game not found";
            g.active = false;
        } 
        return "OK|Game removed: "+ name;
    }

    private String editGame(String[] m){
        if (m.length < 4) return "ERROR|Invalid edit command";
        String name = m[1];
        String field = m[2];
        String value = m[3];

        synchronized (Worker.games) {
            GameInfo g = Worker.games.get(name);
            //TODO : switch fo changes
        }
        return "OK|Game edited: "+ name;
    }

    private String search(String[] m){
        String riskFilter = m.length > 1 ? m[1] : "";
        String betFilter = m.length > 2 ? m[2] : "";
        int minStars = m.length > 3 ? Integer.parseInt(m[3]) : 0;

        StringBuilder sb = new StringBuilder();
        synchronized (Worker.games) {
            for (GameInfo g : Worker.games.values()) {
                if (g.matchesFilter(riskFilter,betFilter,minStars)) {
                    sb.append(g.toString()).append("#");
                }
            }
        }
        return sb.length() > 0 ? sb.toString() : "EMPTY";
    }

    private String play(String[] m) {
        if (m.length < 4) return "ERROR|Invalid play command";
        String playerId  = m[1];
        String gameName  = m[2];
        double betAmount;
        try { betAmount = Double.parseDouble(m[3]); }
        catch (NumberFormatException e) { return "ERROR|Invalid bet amount"; }

        GameInfo game;
        synchronized (Worker.games) {
            game = Worker.games.get(gameName);
        }

        if (game == null || !game.active) 
            return "ERROR|Game not found or inactive";
        if (betAmount < game.minBet || betAmount > game.maxBet) 
            return "ERROR|Bet out of range [" + game.minBet + ", " + game.maxBet + "]";
        
        int random = getSecureRandom(game.gameName, game.hashKey);
        if (random < 0) return "ERROR|SRG communication failed";

        double payout = calculatePayout(random,betAmount,game);

        double houseWinnings = betAmount - payout;
        synchronized (Worker.gameStats) {
            Worker.gameStats.merge(gameName, payout, Double::sum);
        }
        synchronized (Worker.playerStats) {
            Worker.playerStats.merge(playerId, payout - betAmount, Double::sum);
        }

        System.out.printf("[Worker] PLAY %s on %s: bet=%.2f payout=%.2f%n",
                playerId, gameName, betAmount, payout);
        return String.format("PAYOUT|%.2f", payout);

    }
} 