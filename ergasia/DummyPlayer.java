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

    public static void main(String[] args) {
        String masterHost = args.length >= 1 ? args[0] : DEFAULT_MASTER_HOST;
        int masterPort = args.length >= 2 ? Integer.parseInt(args[1]) : DEFAULT_MASTER_PORT;
        System.out.println("========================================");
        System.out.println(" Dummy Player");
        System.out.println(" Connected target: " + masterHost + ":" + masterPort);
        System.out.println("========================================");
        //Connecting to Master
        try (
            Socket socket = new Socket(masterHost, masterPort);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            Scanner sc = new Scanner(System.in)
        ) {
            String playerId = askNonEmpty(sc, "Type Player ID");
            boolean running = true;
            while (running) {
                printMenu();

                System.out.print("Select action: ");
                String choice = sc.nextLine().trim();

                switch (choice) {
                    case "1" -> handleSearch(sc, out, in);
                    case "2" -> handlePlay(sc, out, in, playerId);
                    case "3" -> handleAddBalance(sc, out, in, playerId);
                    case "4" -> {
                        playerId = askNonEmpty(sc, "New Player ID");
                        System.out.println("Connected player switched to : " + playerId);
                    }
                    case "0" -> {
                        running = false;
                        System.out.println("Disconnecting from App");
                    }
                    default -> System.out.println("Invalid Selection");
                }
                System.out.println();
            }
        } catch (IOException e) {
            System.err.println("Failed to connect to Master: " + e.getMessage());
        }
    }

    private static void printMenu() {
        System.out.println("1. Search Games");
        System.out.println("2. Play Game");
        System.out.println("3. Add Balance");
        System.out.println("4. Change Player ID");
        System.out.println("0. Exit");
    }

    //Collects optional filters from the user
    private static void handleSearch(Scanner sc, PrintWriter out, BufferedReader in) throws IOException {
        System.out.println("---- Search Games ----");
        String risk = askOptionalRisk(sc, "Risk level [low / medium / high / * for all]");
        String betCategory = askOptionalBetCategory(sc, "Bet category [$ / $$ / $$$ / * for all]");
        String minStars = askOptionalStars(sc, "Minimum stars [0-5 ή * for all]");

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

    // Sends a request to the Master and prints the response.
    private static void sendAndPrint(String request, PrintWriter out, BufferedReader in) throws IOException {
        System.out.println("[DummyPlayer -> Master] " + request);
        out.println(request);
        String response = in.readLine();
        if (response == null) {
            System.out.println("[Master -> DummyPlayer] No response received.");
            return;
        }
        System.out.println("[Master -> DummyPlayer] " + response);
    }

    // Prompts the user until a non-empty input is given.
    private static String askNonEmpty(Scanner sc, String label) {
        while (true) {
            System.out.print(label + ": ");
            String value = sc.nextLine().trim();
            if (!value.isEmpty()) return value;
            System.out.println("This field cannot be empty");
        }
    }

    // Reads a positive numeric value from the user.
    private static double askPositiveDouble(Scanner sc, String label) {
        while (true) {
            System.out.print(label + ": ");
            String raw = sc.nextLine().trim().replace(',', '.');
            try {
                double value = Double.parseDouble(raw);
                if (value <= 0) {
                    System.out.println("Type bet number");
                    continue;
                }
                return value;
            } catch (NumberFormatException e) {
                System.out.println("Number has to be positive");
            }
        }
    }

    // Reads the desired risk level or '*' if no preference is given.
    private static String askOptionalRisk(Scanner sc, String label) {
        while (true) {
            System.out.print(label + ": ");
            String value = sc.nextLine().trim().toLowerCase(Locale.ROOT);
            if (value.equals("*") || value.isEmpty()) return "*";
            if (value.equals("low") || value.equals("medium") || value.equals("high")) return value;
            System.out.println("Available options: low, medium, high, *");
        }
    }

    private static String askOptionalBetCategory(Scanner sc, String label) {
        while (true) {
            System.out.print(label + ": ");
            String value = sc.nextLine().trim();
            if (value.equals("*") || value.isEmpty()) return "*";
            if (value.equals("$") || value.equals("$$") || value.equals("$$$")) return value;
            System.out.println("Available options: $, $$, $$$, *");
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
            System.out.println("Available options: 0 έως 5 ή *");
        }
    }

    // Formats a double value to string before sending
    private static String formatDouble(double value) {
        if (value == (long) value) {
            return String.format(Locale.US, "%d", (long) value);
        }
        return String.format(Locale.US, "%.2f", value);
    }
}