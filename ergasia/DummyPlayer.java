import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Locale;
import java.util.Scanner;

public class DummyPlayer {

    private static final String DEFAULT_MASTER_HOST = "127.0.0.1";
    private static final int DEFAULT_MASTER_PORT = 5000;
    // TODO in english pls / user should not be able to see internal messages
    public static void main(String[] args) {
        String masterHost = args.length >= 1 ? args[0] : DEFAULT_MASTER_HOST;
        int masterPort = args.length >= 2 ? Integer.parseInt(args[1]) : DEFAULT_MASTER_PORT;

        System.out.println("========================================");
        System.out.println(" Dummy Player");
        System.out.println(" Connected target: " + masterHost + ":" + masterPort);
        System.out.println("========================================");

        try (
            Socket socket = new Socket(masterHost, masterPort);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            Scanner sc = new Scanner(System.in)
        ) {
            String playerId = askNonEmpty(sc, "Δώσε Player ID");

            boolean running = true;
            while (running) {
                printMenu();
                System.out.print("Επιλογή: ");
                String choice = sc.nextLine().trim();

                switch (choice) {
                    case "1" -> handleSearch(sc, out, in);
                    case "2" -> handlePlay(sc, out, in, playerId);
                    case "3" -> handleAddBalance(sc, out, in, playerId);
                    case "4" -> {
                        playerId = askNonEmpty(sc, "Νέο Player ID");
                        System.out.println("Ο ενεργός player έγινε: " + playerId);
                    }
                    case "0" -> {
                        running = false;
                        System.out.println("Έξοδος από Dummy Player.");
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
        System.out.println("1. Search Games");
        System.out.println("2. Play Game");
        System.out.println("3. Add Balance");
        System.out.println("4. Change Player ID");
        System.out.println("0. Exit");
    }

    private static void handleSearch(Scanner sc, PrintWriter out, BufferedReader in) throws IOException {
        System.out.println("---- Search Games ----");

        String risk = askOptionalRisk(sc, "Risk level [low / medium / high / * για όλα]");
        String betCategory = askOptionalBetCategory(sc, "Bet category [$ / $$ / $$$ / * για όλα]");
        String minStars = askOptionalStars(sc, "Minimum stars [0-5 ή * για όλα]");

        String request = "SEARCH|" + risk + "|" + betCategory + "|" + minStars;
        sendAndPrint(request, out, in);
    }

    private static void handlePlay(Scanner sc, PrintWriter out, BufferedReader in, String playerId) throws IOException {
        System.out.println("---- Play Game ----");

        String gameName = askNonEmpty(sc, "Game name");
        double betAmount = askPositiveDouble(sc, "Bet amount");

        String request = "PLAY|" + playerId + "|" + gameName + "|" + formatDouble(betAmount);
        sendAndPrint(request, out, in);
    }

    private static void handleAddBalance(Scanner sc, PrintWriter out, BufferedReader in, String playerId) throws IOException {
        System.out.println("---- Add Balance ----");

        double amount = askPositiveDouble(sc, "Amount to add");

        String request = "ADD_BALANCE|" + playerId + "|" + formatDouble(amount);
        sendAndPrint(request, out, in);
    }

    private static void sendAndPrint(String request, PrintWriter out, BufferedReader in) throws IOException {
        System.out.println("[DummyPlayer -> Master] " + request);
        out.println(request);

        String response = in.readLine();
        if (response == null) {
            System.out.println("[Master -> DummyPlayer] Δεν λήφθηκε απάντηση.");
            return;
        }

        System.out.println("[Master -> DummyPlayer] " + response);
    }

    private static String askNonEmpty(Scanner sc, String label) {
        while (true) {
            System.out.print(label + ": ");
            String value = sc.nextLine().trim();
            if (!value.isEmpty()) return value;
            System.out.println("Το πεδίο δεν μπορεί να είναι κενό.");
        }
    }

    private static double askPositiveDouble(Scanner sc, String label) {
        while (true) {
            System.out.print(label + ": ");
            String raw = sc.nextLine().trim().replace(',', '.');
            try {
                double value = Double.parseDouble(raw);
                if (value <= 0) {
                    System.out.println("Δώσε θετικό αριθμό.");
                    continue;
                }
                return value;
            } catch (NumberFormatException e) {
                System.out.println("Μη έγκυρος αριθμός.");
            }
        }
    }

    private static String askOptionalRisk(Scanner sc, String label) {
        while (true) {
            System.out.print(label + ": ");
            String value = sc.nextLine().trim().toLowerCase(Locale.ROOT);

            if (value.equals("*") || value.isEmpty()) return "*";
            if (value.equals("low") || value.equals("medium") || value.equals("high")) return value;

            System.out.println("Επιτρεπτές τιμές: low, medium, high, *");
        }
    }

    private static String askOptionalBetCategory(Scanner sc, String label) {
        while (true) {
            System.out.print(label + ": ");
            String value = sc.nextLine().trim();

            if (value.equals("*") || value.isEmpty()) return "*";
            if (value.equals("$") || value.equals("$$") || value.equals("$$$")) return value;

            System.out.println("Επιτρεπτές τιμές: $, $$, $$$, *");
        }
    }

    private static String askOptionalStars(Scanner sc, String label) {
        while (true) {
            System.out.print(label + ": ");
            String value = sc.nextLine().trim();

            if (value.equals("*") || value.isEmpty()) return "*";

            try {
                int stars = Integer.parseInt(value);
                if (stars >= 0 && stars <= 5) return String.valueOf(stars);
            } catch (NumberFormatException ignored) {
            }

            System.out.println("Επιτρεπτές τιμές: 0 έως 5 ή *");
        }
    }

    private static String formatDouble(double value) {
        if (value == (long) value) {
            return String.format(Locale.US, "%d", (long) value);
        }
        return String.format(Locale.US, "%.2f", value);
    }
}