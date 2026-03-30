import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Locale;
import java.util.Scanner;

public class ManagerConsole {

    private static final String DEFAULT_MASTER_HOST = "127.0.0.1";
    private static final int DEFAULT_MASTER_PORT = 5000;
    
    //TODO in english pls / better messages and menu pointers
    public static void main(String[] args) {
        String masterHost = args.length >= 1 ? args[0] : DEFAULT_MASTER_HOST;
        int masterPort = args.length >= 2 ? Integer.parseInt(args[1]) : DEFAULT_MASTER_PORT;

        System.out.println("========================================");
        System.out.println(" Manager Console");
        System.out.println(" Connected target: " + masterHost + ":" + masterPort);
        System.out.println("========================================");

        try (
            Socket socket = new Socket(masterHost, masterPort);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            Scanner sc = new Scanner(System.in)
        ) {
            boolean running = true;

            while (running) {
                printMenu();
                System.out.print("Επιλογή: ");
                String choice = sc.nextLine().trim();

                switch (choice) {
                    case "1" -> handleAddGame(sc, out, in);
                    case "2" -> handleEditGame(sc, out, in);
                    case "3" -> handleRemoveGame(sc, out, in);
                    case "4" -> handleStatsProvider(sc, out, in);
                    case "5" -> handleStatsPlayer(sc, out, in);
                    case "0" -> {
                        running = false;
                        System.out.println("Έξοδος από Manager Console.");
                    }
                    default -> System.out.println("Μη έγκυρη επιλογή.");
                }

                System.out.println();
            }

        } catch (IOException e) {
            System.err.println("Αποτυχία σύνδεσης με Master: " + e.getMessage());
        }
    }

    private static void printMenu() {
        System.out.println("1. Add Game");
        System.out.println("2. Edit Game");
        System.out.println("3. Remove Game");
        System.out.println("4. Stats by Provider");
        System.out.println("5. Stats by Player");
        System.out.println("0. Exit");
    }

    private static void handleAddGame(Scanner sc, PrintWriter out, BufferedReader in) throws IOException {
        System.out.println("---- Add Game ----");

        String provider = prompt(sc, "Provider name");
        String gameName = prompt(sc, "Game name");
        double minBet = promptDouble(sc, "Minimum bet");
        String risk = promptRisk(sc, "Risk level (low / medium / high)");
        String hashKey = prompt(sc, "HashKey / Secret key for SRG");

        String json = "{"
                + "\"Provider\":\"" + escapeJson(provider) + "\","
                + "\"GameName\":\"" + escapeJson(gameName) + "\","
                + "\"MinimumBet\":" + formatDouble(minBet) + ","
                + "\"Risk\":\"" + escapeJson(risk) + "\","
                + "\"HashKey\":\"" + escapeJson(hashKey) + "\""
                + "}";

        String request = "ADD_GAME|" + json;
        sendAndPrint(request, out, in);
    }

    private static void handleEditGame(Scanner sc, PrintWriter out, BufferedReader in) throws IOException {
        System.out.println("---- Edit Game ----");

        String gameName = prompt(sc, "Game name");

        System.out.println("Πεδία που μπορείς να αλλάξεις:");
        System.out.println("1. Risk");
        System.out.println("2. MinimumBet");
        System.out.println("3. GameType");

        System.out.print("Επιλογή πεδίου: ");
        String option = sc.nextLine().trim();

        String field;
        String value;

        switch (option) {
            case "1" -> {
                field = "Risk";
                value = promptRisk(sc, "New risk level (low / medium / high)");
            }
            case "2" -> {
                field = "MinimumBet";
                value = formatDouble(promptDouble(sc, "New minimum bet"));
            }
            case "3" -> {
                field = "GameType";
                value = prompt(sc, "New game type/category");
            }
            default -> {
                System.out.println("Μη έγκυρη επιλογή πεδίου.");
                return;
            }
        }

        String request = "EDIT_GAME|" + gameName + "|" + field + "|" + value;
        sendAndPrint(request, out, in);
    }

    private static void handleRemoveGame(Scanner sc, PrintWriter out, BufferedReader in) throws IOException {
        System.out.println("---- Remove Game ----");

        String gameName = prompt(sc, "Game name");
        String request = "REMOVE_GAME|" + gameName;
        sendAndPrint(request, out, in);
    }

    private static void handleStatsProvider(Scanner sc, PrintWriter out, BufferedReader in) throws IOException {
        System.out.println("---- Stats by Provider ----");

        String provider = prompt(sc, "Provider name");
        String request = "STATS_PROVIDER|" + provider;
        sendAndPrint(request, out, in);
    }

    private static void handleStatsPlayer(Scanner sc, PrintWriter out, BufferedReader in) throws IOException {
        System.out.println("---- Stats by Player ----");

        String playerId = prompt(sc, "Player ID");
        String request = "STATS_PLAYER|" + playerId;
        sendAndPrint(request, out, in);
    }

    private static void sendAndPrint(String request, PrintWriter out, BufferedReader in) throws IOException {
        System.out.println("[Manager -> Master] " + request);
        out.println(request);

        String response = in.readLine();
        if (response == null) {
            System.out.println("[Master -> Manager] Δεν λήφθηκε απάντηση.");
            return;
        }

        System.out.println("[Master -> Manager] " + response);
    }

    private static String prompt(Scanner sc, String label) {
        while (true) {
            System.out.print(label + ": ");
            String value = sc.nextLine().trim();
            if (!value.isEmpty()) {
                return value;
            }
            System.out.println("Το πεδίο δεν μπορεί να είναι κενό.");
        }
    }

    private static double promptDouble(Scanner sc, String label) {
        while (true) {
            System.out.print(label + ": ");
            String raw = sc.nextLine().trim().replace(',', '.');
            try {
                double value = Double.parseDouble(raw);
                if (value < 0) {
                    System.out.println("Δώσε μη αρνητικό αριθμό.");
                    continue;
                }
                return value;
            } catch (NumberFormatException e) {
                System.out.println("Μη έγκυρος αριθμός.");
            }
        }
    }

    private static String promptRisk(Scanner sc, String label) {
        while (true) {
            System.out.print(label + ": ");
            String risk = sc.nextLine().trim().toLowerCase(Locale.ROOT);
            if (risk.equals("low") || risk.equals("medium") || risk.equals("high")) {
                return risk;
            }
            System.out.println("Επιτρεπτές τιμές: low, medium, high");
        }
    }

    private static String formatDouble(double value) {
        if (value == (long) value) {
            return String.format(Locale.US, "%d", (long) value);
        }
        return String.format(Locale.US, "%.2f", value);
    }

    private static String escapeJson(String s) {
        return s
                .replace("\\", "\\\\")
                .replace("\"", "\\\"");
    }
}