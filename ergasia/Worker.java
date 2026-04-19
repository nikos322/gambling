import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class Worker {
    private static final Map<String, GameInfo> games = Collections.synchronizedMap(new HashMap<>());
    private static final Map<String, Double> balances = Collections.synchronizedMap(new HashMap<>());
    private static final Map<String, Double> playerTotalBets = Collections.synchronizedMap(new HashMap<>());
    private static final Map<String, Double> playerTotalWins = Collections.synchronizedMap(new HashMap<>());
    private static final Map<String, Integer> providerGameCount = Collections.synchronizedMap(new HashMap<>());
    private static final Map<String, Integer> providerActiveGameCount = Collections.synchronizedMap(new HashMap<>());
    private static final Map<String, Double> providerProfits = Collections.synchronizedMap(new HashMap<>());

    private static final String DEFAULT_SRG_HOST = "127.0.0.1";
    private static final int DEFAULT_SRG_PORT = 7000;

    // Starts the Worker as a TCP server and accepts connections from the Master.
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("Usage: java Worker <port> [srgHost] [srgPort]");
            return;
        }

        int port = Integer.parseInt(args[0]);
        String srgHost = args.length >= 2 ? args[1] : DEFAULT_SRG_HOST;
        int srgPort = args.length >= 3 ? Integer.parseInt(args[2]) : DEFAULT_SRG_PORT;
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("[Worker] Listening on port " + port + " | SRG=" + srgHost + ":" + srgPort);

        while (true) {
            Socket socket = serverSocket.accept();
            new Thread(new Handler(socket, srgHost, srgPort)).start();
        }
    }
    // Handles communication with the Master for each incoming request.
    static class Handler implements Runnable {
        private final Socket socket;
        private final String srgHost;
        private final int srgPort;
        Handler(Socket socket, String srgHost, int srgPort) {
            this.socket = socket;
            this.srgHost = srgHost;
            this.srgPort = srgPort;
        }

        @Override
        // Listens for incoming requests, processes them and sends back responses.
        public void run() {
            try (
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
            ) {
                String line;
                while ((line = in.readLine()) != null) {
                    System.out.println(line);
                    String response = handleCommand(line);
                    out.println(response);
                }
            } catch (IOException e) {
                System.err.println("[Worker] Handler error: " + e.getMessage());
            } finally {
                try {
                    socket.close();
                } catch (IOException ignored) {}
            }
        }

        private String handleCommand(String raw) {
            try {
                if (raw == null || raw.isBlank()) {
                    return "ERROR|Empty command";
                }

                String[] parts2 = raw.split("\\|", 2);
                String cmd = parts2[0];
                //Matches user's choice to the right handler
                return switch (cmd) {
                    case "ADD_GAME" -> handleAddGame(parts2.length > 1 ? parts2[1] : "");
                    case "REMOVE_GAME" -> handleRemoveGame(parts2.length > 1 ? parts2[1] : "");
                    case "EDIT_GAME" -> handleEditGame(raw.split("\\|"));
                    case "SEARCH" -> handleSearch(raw.split("\\|"));
                    case "PLAY" -> handlePlay(raw.split("\\|"));
                    case "ADD_BALANCE" -> handleAddBalance(raw.split("\\|"));
                    case "MAP_SEARCH" -> handleMapSearch(raw.split("\\|"));
                    case "MAP_PROVIDER" -> handleMapProvider(raw.split("\\|"));
                    case "MAP_PLAYER" -> handleMapPlayer(raw.split("\\|"));
                    default -> "ERROR|Unknown command: " + cmd;
                };
            } catch (Exception e) {
                return "ERROR|" + e.getMessage();
            }
        }

        private String handleAddGame(String json) {
            String gameName = readJsonString(json, "GameName");
            if (gameName.isEmpty()) {
                return "ERROR|Missing GameName";
            }
            String providerName = firstNonEmpty(
                    readJsonString(json, "ProviderName"),
                    readJsonString(json, "Provider")
            );
            int stars = readJsonInt(json, "Stars", 0);
            int noOfVotes = readJsonInt(json, "NoOfVotes", 0);
            String gameLogo = readJsonString(json, "GameLogo");

            double minBet = firstNonNegative(
                    readJsonDoubleMaybe(json, "MinBet"),
                    readJsonDoubleMaybe(json, "MinimumBet"),
                    0.0
            );
            double maxBet = firstNonNegative(
                    readJsonDoubleMaybe(json, "MaxBet"),
                    minBet * 10.0,
                    minBet
            );

            String riskLevel = firstNonEmpty(
                    readJsonString(json, "RiskLevel"),
                    readJsonString(json, "Risk")
            );
            if (riskLevel.isEmpty()) {
                riskLevel = "low";
            }

            String hashKey = readJsonString(json, "HashKey");
            if (hashKey.isEmpty()) {
                hashKey = readJsonString(json, "Secret");
            }

            /*Updates the provider information when a game is added or modified.
            It handles new games, provider changes, and reactivation of inactive games.*/
            GameInfo game = new GameInfo(
                    gameName,
                    providerName,
                    stars,
                    noOfVotes,
                    gameLogo,
                    minBet,
                    maxBet,
                    riskLevel,
                    hashKey
            );

            synchronized (games) {
                GameInfo existing = games.get(gameName);
                games.put(gameName, game);

                if (existing == null) {
                    providerGameCount.put(providerName, providerGameCount.getOrDefault(providerName, 0) + 1);
                    providerActiveGameCount.put(providerName, providerActiveGameCount.getOrDefault(providerName, 0) + 1);
                } else {
                    if (!existing.providerName.equals(providerName)) {
                        providerActiveGameCount.put(
                                existing.providerName,
                                Math.max(0, providerActiveGameCount.getOrDefault(existing.providerName, 0) - (existing.active ? 1 : 0))
                        );
                        providerActiveGameCount.put(
                                providerName,
                                providerActiveGameCount.getOrDefault(providerName, 0) + 1
                        );
                    } else if (!existing.active) {
                        providerActiveGameCount.put(
                                providerName,
                                providerActiveGameCount.getOrDefault(providerName, 0) + 1
                        );
                    }
                }
            }

            return "OK|Game added|" + gameName;
        }

        private String handleRemoveGame(String gameName) {
            if (gameName == null || gameName.isBlank()) {
                return "ERROR|Missing game name";
            }
            synchronized (games) {
                GameInfo g = games.get(gameName);
                if (g == null) {
                    return "ERROR|Game not found";
                }
                if (!g.active) {
                    return "OK|Already inactive|" + gameName;
                }
                //A game never gets deleted, it becomes inactive and game count gets redacted)
                g.active = false;
                providerActiveGameCount.put(
                        g.providerName,
                        Math.max(0, providerActiveGameCount.getOrDefault(g.providerName, 0) - 1)
                );
            }
            return "OK|Game inactive|" + gameName;
        }

        private String handleEditGame(String[] p) {
            if (p.length < 4) {
                return "ERROR|Usage: EDIT_GAME|gameName|field|value";
            }
            String gameName = p[1];
            String field = p[2];
            String value = p[3];

            synchronized (games) {
                GameInfo g = games.get(gameName);
                if (g == null) {
                    return "ERROR|Game not found";
                }
                switch (field.toLowerCase(Locale.ROOT)) {
                    case "risk", "risklevel" -> {
                        g.riskLevel = value.toLowerCase(Locale.ROOT);
                        g.jackpot = computeJackpot(g.riskLevel);
                    }
                    case "minbet", "minimumbet" -> {
                        g.minBet = Double.parseDouble(value);
                        g.betCategory = computeBetCategory(g.minBet);
                    }
                    case "maxbet" -> g.maxBet = Double.parseDouble(value);
                    case "gametype", "category" -> {
                        return "OK|Field accepted but not stored separately";
                    }
                    case "hashkey", "secret" -> g.hashKey = value;
                    case "stars" -> g.stars = Integer.parseInt(value);
                    case "active" -> g.active = Boolean.parseBoolean(value);
                    default -> {
                        return "ERROR|Unsupported field: " + field;
                    }
                }
            }
            return "OK|Game updated|" + gameName;
        }

        private String handleSearch(String[] p) {
            if (p.length < 4) {
                return "ERROR|Usage: SEARCH|risk|betCategory|minStars";
            }
            String risk = p[1];
            String bet = p[2];
            int minStars = parseStars(p[3]);
            List<String> results = new ArrayList<>();

            synchronized (games) {
                for (GameInfo g : games.values()) {
                    if (g.matchesFilter(risk, bet, minStars)) {
                        results.add(formatGame(g));
                    }
                }
            }

            if (results.isEmpty()) {
                return "EMPTY";
            }
            return String.join("##", results);
        }

        private String handleAddBalance(String[] p) {
            if (p.length < 3) {
                return "ERROR|Usage: ADD_BALANCE|playerId|amount";
            }
            String playerId = p[1];
            double amount = Double.parseDouble(p[2]);

            if (amount <= 0) {
                return "ERROR|Amount must be positive";
            }
            double newBalance;
            synchronized (balances) {
                newBalance = balances.getOrDefault(playerId, 0.0) + amount;
                balances.put(playerId, newBalance);
            }
            return "OK|Balance=" + format2(newBalance);
        }
        /* Handles the betting process:
             1. Validates game and player balance
             2. Requests a random number from the SRG server
             3. Verifies the hash for security
             4. Calculates payout based on risk rules
             5. Updates player balance and statistics*/
        private String handlePlay(String[] p) {
            if (p.length < 4) {
                return "ERROR|Usage: PLAY|playerId|gameName|betAmount";
            }
            String playerId = p[1];
            String gameName = p[2];
            double betAmount = Double.parseDouble(p[3]);
            GameInfo game;
            synchronized (games) {
                game = games.get(gameName);
            }
            if (game == null) {
                return "ERROR|Game not found";
            }
            if (!game.active) {
                return "ERROR|Game inactive";
            }
            if (betAmount < game.minBet) {
                return "ERROR|Bet below minimum";
            }
            if (betAmount > game.maxBet) {
                return "ERROR|Bet above maximum";
            }

            synchronized (balances) {
                double balance = balances.getOrDefault(playerId, 0.0);
                if (balance < betAmount) {
                    return "ERROR|Insufficient balance";
                }
                balances.put(playerId, balance - betAmount);
            }

            String srgResponse = requestRandomFromSRG(gameName, game.hashKey);
            if (srgResponse.startsWith("ERROR")) {
                refund(playerId, betAmount);
                return srgResponse;
            }
            String[] srgParts = srgResponse.split("\\|");
            if (srgParts.length < 2) {
                refund(playerId, betAmount);
                return "ERROR|Invalid SRG response";
            }
            long randomNumber;
            try {
                randomNumber = Long.parseLong(srgParts[0].trim());
            } catch (NumberFormatException e) {
                refund(playerId, betAmount);
                return "ERROR|Invalid SRG number";
            }
            String receivedHash = srgParts[1].trim();
            String expectedHash = sha256(srgParts[0].trim() + game.hashKey);

            if (!expectedHash.equalsIgnoreCase(receivedHash)) {
                refund(playerId, betAmount);
                return "ERROR|Hash verification failed";
            }

            long mod100 = Math.abs(randomNumber % 100);
            long mod10 = Math.abs(randomNumber % 10);
            boolean jackpotWin = (mod100 == 0);
            double payout;

            if (jackpotWin) {
                payout = betAmount * game.jackpot;
            } else {
                double multiplier = getMultiplier(game.riskLevel, (int) mod10);
                payout = betAmount * multiplier;
            }

            synchronized (balances) {
                balances.put(playerId, balances.getOrDefault(playerId, 0.0) + payout);
                playerTotalBets.put(playerId, playerTotalBets.getOrDefault(playerId, 0.0) + betAmount);
                playerTotalWins.put(playerId, playerTotalWins.getOrDefault(playerId, 0.0) + payout);
            }
            synchronized (providerProfits) {
                providerProfits.put(game.providerName, providerProfits.getOrDefault(game.providerName, 0.0) + (betAmount - payout));
            }
            double finalBalance;
            synchronized (balances) {
                finalBalance = balances.getOrDefault(playerId, 0.0);
            }
            String resultType = jackpotWin ? "JACKPOT" : (payout > 0 ? "WIN" : "LOSE");

            return "OK|" + resultType
                    + "|Payout=" + format2(payout)
                    + "|Balance=" + format2(finalBalance)
                    + "|Random=" + randomNumber
                    + "|Mod100=" + mod100
                    + "|Mod10=" + mod10;
        }
        //Receives user's search and sends partial results to the Reducer.
        private String handleMapSearch(String[] p) {
            if (p.length < 7) {
                return "ERROR|Invalid MAP_SEARCH";
            }
            String jobId = p[1];
            String risk = p[2];
            String bet = p[3];
            int minStars = parseStars(p[4]);
            String reducerHost = p[5];
            int reducerPort = Integer.parseInt(p[6]);
            List<String> results = new ArrayList<>();
            synchronized (games) {
                for (GameInfo g : games.values()) {
                    if (g.matchesFilter(risk, bet, minStars)) {
                        results.add(formatGame(g));
                    }
                }
            }
            String partial = results.isEmpty() ? "EMPTY" : String.join("##", results);
            sendToReducer(reducerHost, reducerPort, "PARTIAL|" + jobId + "|" + partial);
            return "OK";
        }
        // Sends partial provider statistics to the Reducer for aggregation.
        private String handleMapProvider(String[] p) {
            if (p.length < 5) {
                return "ERROR|Invalid MAP_PROVIDER";
            }
            String jobId = p[1];
            String providerName = p[2];
            String reducerHost = p[3];
            int reducerPort = Integer.parseInt(p[4]);
            int total = providerGameCount.getOrDefault(providerName, 0);
            int active = providerActiveGameCount.getOrDefault(providerName, 0);
            double profit = providerProfits.getOrDefault(providerName, 0.0);
            
            String partial = "PROVIDER=" + providerName + ",TOTAL=" + total + ",ACTIVE=" + active + ",PROFIT=" + profit;
            sendToReducer(reducerHost, reducerPort, "PARTIAL|" + jobId + "|" + partial);
            return "OK";
        }

        // Sends partial player statistics (bets, wins, balance) to the Reducer.
        private String handleMapPlayer(String[] p) {
            if (p.length < 5) {
                return "ERROR|Invalid MAP_PLAYER";
            }
            String jobId = p[1];
            String playerId = p[2];
            String reducerHost = p[3];
            int reducerPort = Integer.parseInt(p[4]);

            double balance = balances.getOrDefault(playerId, 0.0);
            double totalBets = playerTotalBets.getOrDefault(playerId, 0.0);
            double totalWins = playerTotalWins.getOrDefault(playerId, 0.0);

            String partial = "PLAYER=" + playerId
                    + ",BALANCE=" + format2(balance)
                    + ",TOTAL_BETS=" + format2(totalBets)
                    + ",TOTAL_WINS=" + format2(totalWins);
            sendToReducer(reducerHost, reducerPort, "PARTIAL|" + jobId + "|" + partial);
            return "OK";
        }

        private void refund(String playerId, double amount) {
            synchronized (balances) {
                balances.put(playerId, balances.getOrDefault(playerId, 0.0) + amount);
            }
        }

        private void sendToReducer(String host, int port, String msg) {
            try (
                Socket s = new Socket(host, port);
                PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()))
            ) {
                out.println(msg);
                in.readLine();
            } catch (IOException e) {
                System.err.println("[Worker] Reducer send error: " + e.getMessage());
            }
        }

        private String requestRandomFromSRG(String gameName, String secret) {
            try (
                Socket s = new Socket(srgHost, srgPort);
                PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()))
            ) {
                out.println("GET_RANDOM|" + gameName + "|" + secret);
                String resp = in.readLine();
                if (resp == null) {
                    return "ERROR|No response from SRG";
                }
                return resp;
            } catch (IOException e) {
                return "ERROR|SRG connection failed";
            }
        }

        // Returns the payout multiplier based on risk level and random index.
        private double getMultiplier(String riskLevel, int index) {
            double[] table;
            switch (riskLevel.toLowerCase()) {
                case "low" -> table = new double[]{0.0, 0.0, 0.0, 0.1, 0.5, 1.0, 1.1, 1.3, 2.0, 2.5};
                case "medium" -> table = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.5, 1.0, 1.5, 2.5, 3.5};
                case "high" -> table = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 2.0, 6.5};
                default -> table = new double[]{0.0, 0.0, 0.0, 0.1, 0.5, 1.0, 1.1, 1.3, 2.0, 2.5};
            }
            if (index < 0 || index >= table.length) {
                return 0.0;
            }
            return table[index];
        }

        // Converts game data into a string for transmission.
        private String formatGame(GameInfo g) {
            return "GameName=" + g.gameName
                    + ",ProviderName=" + g.providerName
                    + ",Stars=" + g.stars
                    + ",MinBet=" + format2(g.minBet)
                    + ",MaxBet=" + format2(g.maxBet)
                    + ",RiskLevel=" + g.riskLevel
                    + ",BetCategory=" + g.betCategory
                    + ",Jackpot=" + format2(g.jackpot);
        }

        private int parseStars(String raw) {
            if (raw == null || raw.equals("*")) {
                return 0;
            }
            try {
                return Integer.parseInt(raw);
            } catch (NumberFormatException e) {
                return 0;
            }
        }

        private String readJsonString(String json, String key) {
            String quotedKey = "\"" + key + "\"";
            int keyPos = json.indexOf(quotedKey);
            if (keyPos < 0) {
                return "";
            }
            int colonPos = json.indexOf(':', keyPos);
            if (colonPos < 0) {
                return "";
            }
            int firstQuote = json.indexOf('"', colonPos + 1);
            if (firstQuote < 0) {
                return "";
            }
            int secondQuote = json.indexOf('"', firstQuote + 1);
            if (secondQuote < 0) {
                return "";
            }
            return json.substring(firstQuote + 1, secondQuote).trim();
        }

        private int readJsonInt(String json, String key, int defaultValue) {
            try {
                Double d = readJsonDoubleMaybe(json, key);
                return d == null ? defaultValue : d.intValue();
            } catch (Exception e) {
                return defaultValue;
            }
        }

        private Double readJsonDoubleMaybe(String json, String key) {
            String quotedKey = "\"" + key + "\"";
            int keyPos = json.indexOf(quotedKey);
            if (keyPos < 0) {
                return null;
            }
            int colonPos = json.indexOf(':', keyPos);
            if (colonPos < 0) {
                return null;
            }
            int start = colonPos + 1;
            while (start < json.length() && Character.isWhitespace(json.charAt(start))) {
                start++;
            }
            int end = start;
            while (end < json.length()) {
                char c = json.charAt(end);
                if ((c >= '0' && c <= '9') || c == '.' || c == '-') {
                    end++;
                } else {
                    break;
                }
            }
            if (end <= start) {
                return null;
            }
            return Double.valueOf(json.substring(start, end));
        }

        private String firstNonEmpty(String a, String b) {
            return (a != null && !a.isBlank()) ? a : (b == null ? "" : b);
        }

        private double firstNonNegative(Double a, Double b, double fallback) {
            if (a != null && a >= 0) {
                return a;
            }
            if (b != null && b >= 0) {
                return b;
            }
            return fallback;
        }

        private String format2(double d) {
            return String.format(Locale.US, "%.2f", d);
        }

        private double computeJackpot(String riskLevel) {
            return switch (riskLevel.toLowerCase()) {
                case "low" -> 10.0;
                case "medium" -> 20.0;
                case "high" -> 40.0;
                default -> 10.0;
            };
        }

        private String computeBetCategory(double minBet) {
            if (minBet >= 5.0) return "$$$";
            if (minBet >= 1.0) return "$$";
            return "$";
        }
        // Computes SHA-256 hash used to verify SRG responses.
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