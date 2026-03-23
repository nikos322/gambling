import java.io.*;
import java.net.*;
import java.security.MessageDigest;
import java.util.*;
import jdk.jshell.spi.ExecutionControl;

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
            case "MAP_STATS_PROVIDER" -> mapAndSendToReducer(p,"PROVIDER");
            case "MAP_STATS_PLAYER" -> mapAndSendToReducer(p, "PLAYER");
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
            //TODO : switch for changes
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
            Worker.gameStats.merge(gameName, houseWinnings, Double::sum);
        }
        synchronized (Worker.playerStats) {
            Worker.playerStats.merge(playerId, payout - betAmount, Double::sum);
        }

        System.out.printf("[Worker] PLAY %s on %s: bet=%.2f payout=%.2f%n",
                playerId, gameName, betAmount, payout);
        return String.format("PAYOUT|%.2f", payout);

    } 

    private String mapAndSendToReducer(String[] p, String type) {
        if (p.length < 5) return "ERROR|Missing params for MAP";
 
        String jobId       = p[1];
        String queryParam  = p[2]; // providerName ή playerId
        String reducerHost = p[3];
        int    reducerPort = Integer.parseInt(p[4]);
 
        // Υπολογισμός map result
        String mapResult = type.equals("PROVIDER")
                ? computeProviderMap(queryParam)
                : computePlayerMap(queryParam);
 
        // Αποστολή αποτελέσματος ΑΠΕΥΘΕΙΑΣ στον Reducer
        sendToReducer(reducerHost, reducerPort, jobId, mapResult);
 
        // Επιστροφή acknowledgment στον Master
        return "OK|Map result sent to reducer for job " + jobId;
    }
 
    // MAP για provider: συγκεντρώνει house P&L ανά game για έναν provider
    private String computeProviderMap(String providerName) {
        StringBuilder sb = new StringBuilder();
        synchronized (Worker.games) {
            for (Map.Entry<String, GameInfo> e : Worker.games.entrySet()) {
                if (e.getValue().providerName.equals(providerName)) {
                    double stat = Worker.gameStats.getOrDefault(e.getKey(), 0.0);
                    sb.append(e.getKey()).append("=").append(stat).append(",");
                }
            }
        }
        return sb.length() > 0 ? sb.toString() : "EMPTY";
    }
 
    // MAP για player: επιστρέφει το P&L του παίκτη από αυτόν τον Worker
    private String computePlayerMap(String playerId) {
        synchronized (Worker.playerStats) {
            double total = Worker.playerStats.getOrDefault(playerId, 0.0);
            return total != 0.0 ? playerId + "=" + total : "EMPTY";
        }
    }
 
    // TCP αποστολή MAP_RESULT απευθείας στον Reducer
    private void sendToReducer(String host, int port, String jobId, String mapResult) {
        try (Socket reducerSocket = new Socket(host, port);
             PrintWriter out = new PrintWriter(reducerSocket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(
                     new InputStreamReader(reducerSocket.getInputStream()))) {
 
            out.println("MAP_RESULT|" + jobId + "|" + mapResult);
            String ack = in.readLine();
            System.out.println("[Worker] Reducer ack for job " + jobId + ": " + ack);
 
        } catch (IOException e) {
            System.err.println("[Worker] Failed to send map result to Reducer: " + e.getMessage());
        }
    }

    private int getSecureRandom(String gameName, String secret){
        try(Socket srgSocket = new Socket(srgHost,srgPort);
            BufferedReader srgIn = new BufferedReader(new InputStreamReader(srgSocket.getInputStream()));
            PrintWriter srgOut = new PrintWriter(srgSocket.getOutputStream(),true)) 
            {
                srgOut.println("GET_RANDOM|"+ gameName);
                String response = srgIn.readLine();

                if (response == null) return -1;
                String[] parts = response.split("\\|");
                if (parts.length < 2) return -1;

                int number = Integer.parseInt(parts[0]);
                String hash = parts[1];

                String myHash = sha256(number + secret);
                if (!myHash.equals(hash)) {
                    System.err.println("[Worker] SRG hash mismatch");
                    return -1;
                }
                return number;
                
            } catch (Exception exception){
                System.err.println("[Worker] Srg error: "+ exception.getMessage());
                return -1;
            }
    }

    private double calculatePayout(int randomNum, double bet, GameInfo game){
        int mod100 = randomNum % 100;
        if (mod100 == 0) {
            System.out.println("[Worker] Jackpot!!!!");
            return bet * game.jackpot;
        }

        int idx = randomNum % 10;
        double multiplier = game.getMultipliers()[idx];
        return bet * multiplier;
    }

    private String sha256(String input){
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