import java.io.*;
import java.net.*;
import java.util.*;

public class Reducer {

    private static final Map<String, ReduceJob> jobs = Collections.synchronizedMap(new HashMap<>());

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("Usage: java Reducer <port>");
            return;
        }

        int port = Integer.parseInt(args[0]);

        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("[Reducer] Running on port " + port);

        while (true) {
            Socket socket = serverSocket.accept();
            new Thread(new Handler(socket)).start();
        }
    }

    static class Handler implements Runnable {
        private final Socket socket;

        Handler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try (
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
            ) {
                String msg;
                while ((msg = in.readLine()) != null) {
                    System.out.println(msg);
                    out.println(handle(msg));
                }

            } catch (IOException e) {
                System.err.println("[Reducer] Handler error: " + e.getMessage());
            } finally {
                try {
                    socket.close();
                } catch (IOException ignored) {}
            }
        }

        private String handle(String msg) {
            if (msg == null || msg.isBlank()) {
                return "ERROR|Empty reducer command";
            }

            String[] p = msg.split("\\|", 3);
            String cmd = p[0];

            return switch (cmd) {
                case "INIT_REDUCE_SEARCH" -> init(msg.split("\\|"));
                case "INIT_REDUCE_PROVIDER" -> init(msg.split("\\|"));
                case "INIT_REDUCE_PLAYER" -> init(msg.split("\\|"));
                case "PARTIAL" -> addPartial(msg.split("\\|", 3));
                case "GET_RESULT" -> getResult(msg.split("\\|"));
                default -> "ERROR|Unknown reducer command";
            };
        }

        private String init(String[] p) {
            if (p.length < 3) {
                return "ERROR|Invalid INIT";
            }

            String jobId = p[1];
            int expected = Integer.parseInt(p[2]);

            synchronized (jobs) {
                jobs.put(jobId, new ReduceJob(jobId, expected));
            }

            return "OK|INIT|" + jobId;
        }

        // TODO ti kanei auto
        private String addPartial(String[] p) {
            if (p.length < 3) {
                return "ERROR|Invalid PARTIAL";
            }

            String jobId = p[1];
            String result = p[2];

            ReduceJob job;
            synchronized (jobs) {
                job = jobs.get(jobId);
            }

            if (job == null) {
                return "ERROR|Unknown jobId";
            }

            job.add(result);
            return "OK|PARTIAL|" + jobId;
        }

        private String getResult(String[] p) {
            if (p.length < 2) {
                return "ERROR|Invalid GET_RESULT";
            }

            String jobId = p[1];

            ReduceJob job;
            synchronized (jobs) {
                job = jobs.get(jobId);
            }

            if (job == null) {
                return "ERROR|Unknown jobId";
            }

            String result = job.waitAndReduce();

            synchronized (jobs) {
                jobs.remove(jobId);
            }

            return result;
        }
    }

    static class ReduceJob {
        private final String jobId;
        private final int expected;
        private final List<String> results = new ArrayList<>();

        ReduceJob(String jobId, int expected) {
            this.jobId = jobId;
            this.expected = expected;
        }

        synchronized void add(String r) {
            results.add(r);
            notifyAll();
        }

        synchronized String waitAndReduce() {
            while (results.size() < expected) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return "ERROR|Reducer interrupted|" + jobId;
                }
            }

            List<String> nonEmpty = new ArrayList<>();
            for (String r : results) {
                if (r != null && !r.isBlank() && !r.equals("EMPTY")) {
                    nonEmpty.add(r);
                }
            }

            if (jobId.startsWith("job-search-")) {
                return nonEmpty.isEmpty()
                        ? "SEARCH_RESULTS|EMPTY"
                        : "SEARCH_RESULTS|" + String.join("##", nonEmpty);
            }

           if (jobId.startsWith("job-prov-")) {
            // TODO add profit
                int totalGames = 0;
                int activeGames = 0;
                String provider = "";

                for (String r : nonEmpty) {
                    String[] parts = r.split(",");

                    for (String part : parts) {
                        if (part.startsWith("PROVIDER=")) {
                            provider = part.split("=")[1];
                        } else if (part.startsWith("TOTAL=")) {
                            totalGames += Integer.parseInt(part.split("=")[1]);
                        } else if (part.startsWith("ACTIVE=")) {
                            activeGames += Integer.parseInt(part.split("=")[1]);
                        }
                    }
                }

                return "PROVIDER_STATS|Provider=" + provider
                        + "|TotalGames=" + totalGames
                        + "|ActiveGames=" + activeGames;
                }

           if (jobId.startsWith("job-player-")) {

                double totalBets = 0;
                double totalWins = 0;
                double balance = 0;
                String player = "";

                for (String r : nonEmpty) {
                    String[] parts = r.split(",");

                    for (String part : parts) {
                        if (part.startsWith("PLAYER=")) {
                            player = part.split("=")[1];
                        } else if (part.startsWith("TOTAL_BETS=")) {
                            totalBets += Double.parseDouble(part.split("=")[1]);
                        } else if (part.startsWith("TOTAL_WINS=")) {
                            totalWins += Double.parseDouble(part.split("=")[1]);
                        } else if (part.startsWith("BALANCE=")) {
                            balance += Double.parseDouble(part.split("=")[1]);
                        }
                    }
                }

                double net = totalWins - totalBets;

                return "PLAYER_STATS|Player=" + player
                        + "|TotalBets=" + totalBets
                        + "|TotalWins=" + totalWins
                        + "|Net=" + net
                        + "|Balance=" + balance;
            }

            return nonEmpty.isEmpty() ? "EMPTY" : String.join("##", nonEmpty);
        }
    }
}