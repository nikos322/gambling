import java.io.*;
import java.net.*;
import java.util.*;
 
/**
 * Reducer TCP Server
 *
 * Νέος flow (σωστός MapReduce):
 *
 *  1. Master στέλνει στον Reducer: "INIT_REDUCE_PROVIDER|jobId|N"
 *     (N = αριθμός Workers που θα στείλουν αποτελέσματα)
 *  2. Master στέλνει σε κάθε Worker:
 *     "MAP_PROVIDER|jobId|providerName|reducerHost|reducerPort"
 *  3. Κάθε Worker υπολογίζει map result και το στέλνει ΑΠΕΥΘΕΙΑΣ στον Reducer:
 *     "MAP_RESULT|jobId|game1=1000.0,game2=-50.0"
 *  4. Reducer μαζεύει N αποτελέσματα, εκτελεί reduce, κρατά τελικό αποτέλεσμα.
 *  5. Master στέλνει: "GET_RESULT|jobId" — παίρνει τελικό αποτέλεσμα (blocking).
 *
 *  Ίδιος flow για PLAYER queries.
 */
public class Reducer {
 
    private static final int DEFAULT_PORT = 6000;
 
    // jobId -> ReduceJob (ενεργά jobs)
    static final Map<String, ReduceJob> jobs = new HashMap<>();
 
    public static void main(String[] args) throws IOException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("[Reducer] Listening on port " + port);
 
        while (true) {
            Socket socket = serverSocket.accept();
            new Thread(new ReducerHandler(socket)).start();
        }
    }
 
    static synchronized ReduceJob createJob(String jobId, int expectedWorkers, String type) {
        ReduceJob job = new ReduceJob(jobId, expectedWorkers, type);
        jobs.put(jobId, job);
        return job;
    }
 
    static synchronized ReduceJob getJob(String jobId) {
        return jobs.get(jobId);
    }
 
    static synchronized void removeJob(String jobId) {
        jobs.remove(jobId);
    }
}
 
 
// =================================================================
// ReduceJob — κρατά κατάσταση ενός MapReduce job
// Συγχρονισμός με synchronized + wait/notify
// =================================================================
class ReduceJob {
 
    final String jobId;
    final int    expectedWorkers;
    final String type; // "PROVIDER" ή "PLAYER"
 
    private final List<String> mapResults = new ArrayList<>();
    private String  finalResult = null;
    private boolean done        = false;
 
    public ReduceJob(String jobId, int expectedWorkers, String type) {
        this.jobId           = jobId;
        this.expectedWorkers = expectedWorkers;
        this.type            = type;
    }
 
    // Καλείται από ReducerHandler όταν φτάσει MAP_RESULT από Worker
    public synchronized void addMapResult(String result) {
        mapResults.add(result);
        System.out.printf("[ReduceJob %s] Received %d/%d map results%n",
                jobId, mapResults.size(), expectedWorkers);
 
        if (mapResults.size() >= expectedWorkers) {
            // Όλοι οι Workers έστειλαν — εκτέλεσε reduce
            finalResult = reduce(mapResults, type);
            done = true;
            notifyAll(); // ξύπνα τον Master που περιμένει στο GET_RESULT
        }
    }
 
    // Master καλεί αυτό — μπλοκάρει μέχρι done == true
    public synchronized String waitForResult() throws InterruptedException {
        while (!done) {
            wait();
        }
        return finalResult;
    }
 
    // -----------------------------------------------------------------
    // Reduce logic
    // -----------------------------------------------------------------
    private String reduce(List<String> results, String type) {
        if ("PROVIDER".equals(type)) return reduceProvider(results);
        if ("PLAYER".equals(type))   return reducePlayer(results);
        return "ERROR|Unknown reduce type";
    }
 
    /**
     * PROVIDER reduce
     * Κάθε Worker έστειλε: "game1=1000.0,game2=-50.0"
     * Output: "game1: +1000.00 FUN, game2: -50.00 FUN, Total: +950.00 FUN"
     */
    private String reduceProvider(List<String> results) {
        Map<String, Double> aggregated = new LinkedHashMap<>();
 
        for (String workerResult : results) {
            if (workerResult == null || workerResult.equals("EMPTY")) continue;
            for (String entry : workerResult.split(",")) {
                if (entry.isEmpty()) continue;
                String[] kv = entry.split("=");
                if (kv.length < 2) continue;
                try {
                    aggregated.merge(kv[0].trim(), Double.parseDouble(kv[1].trim()), Double::sum);
                } catch (NumberFormatException ignored) {}
            }
        }
 
        if (aggregated.isEmpty()) return "No data found for this provider";
 
        StringBuilder sb = new StringBuilder();
        double total = 0;
        for (Map.Entry<String, Double> e : aggregated.entrySet()) {
            double v = e.getValue();
            total += v;
            sb.append(e.getKey()).append(": ")
              .append(v >= 0 ? "+" : "").append(String.format("%.2f", v))
              .append(" FUN, ");
        }
        sb.append("Total: ").append(total >= 0 ? "+" : "")
          .append(String.format("%.2f", total)).append(" FUN");
        return sb.toString();
    }
 
    /**
     * PLAYER reduce
     * Κάθε Worker έστειλε: "user123=-50.0" (ή "EMPTY")
     * Output: "user123 — Total Profit/Loss: -50.00 FUN"
     */
    private String reducePlayer(List<String> results) {
        String playerId = null;
        double total    = 0;
 
        for (String workerResult : results) {
            if (workerResult == null || workerResult.equals("EMPTY")) continue;
            String[] kv = workerResult.split("=");
            if (kv.length < 2) continue;
            if (playerId == null) playerId = kv[0].trim();
            try { total += Double.parseDouble(kv[1].trim()); }
            catch (NumberFormatException ignored) {}
        }
 
        if (playerId == null) return "No data found for this player";
        return playerId + " — Total Profit/Loss: "
                + (total >= 0 ? "+" : "") + String.format("%.2f", total) + " FUN";
    }
}
 
 
// =================================================================
// ReducerHandler — εξυπηρετεί συνδέσεις από Master ΚΑΙ Workers
// =================================================================
class ReducerHandler implements Runnable {
 
    private final Socket socket;
 
    public ReducerHandler(Socket socket) {
        this.socket = socket;
    }
 
    @Override
    public void run() {
        try (
            BufferedReader in  = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter    out = new PrintWriter(socket.getOutputStream(), true)
        ) {
            String line;
            while ((line = in.readLine()) != null) {
                String response = dispatch(line);
                if (response != null) out.println(response);
            }
        } catch (IOException e) {
            System.err.println("[ReducerHandler] " + e.getMessage());
        } finally {
            try { socket.close(); } catch (IOException ignored) {}
        }
    }
 
    private String dispatch(String msg) {
        String[] p = msg.split("\\|", 4);
        switch (p[0]) {
            case "INIT_REDUCE_PROVIDER": return handleInit(p, "PROVIDER");
            case "INIT_REDUCE_PLAYER":   return handleInit(p, "PLAYER");
            case "MAP_RESULT":           return handleMapResult(p);
            case "GET_RESULT":           return handleGetResult(p);
            default: return "ERROR|Unknown command: " + p[0];
        }
    }
 
    /**
     * Από Master:
     *   "INIT_REDUCE_PROVIDER|jobId|numberOfWorkers"
     *   "INIT_REDUCE_PLAYER|jobId|numberOfWorkers"
     */
    private String handleInit(String[] p, String type) {
        if (p.length < 3) return "ERROR|Missing params";
        String jobId = p[1];
        int n;
        try { n = Integer.parseInt(p[2]); }
        catch (NumberFormatException e) { return "ERROR|Invalid worker count"; }
 
        Reducer.createJob(jobId, n, type);
        System.out.printf("[Reducer] Job %s created (%s, expecting %d workers)%n", jobId, type, n);
        return "OK|Job created: " + jobId;
    }
 
    /**
     * Από Worker (απευθείας TCP):
     *   "MAP_RESULT|jobId|game1=100.0,game2=-20.0"
     */
    private String handleMapResult(String[] p) {
        if (p.length < 3) return "ERROR|Missing params";
        String jobId  = p[1];
        String result = p[2];
 
        ReduceJob job = Reducer.getJob(jobId);
        if (job == null) return "ERROR|Unknown jobId: " + jobId;
 
        job.addMapResult(result);
        return "OK|Result received";
    }
 
    /**
     * Από Master: "GET_RESULT|jobId"
     * Μπλοκάρει (wait) μέχρι να τελειώσει το reduce.
     */
    private String handleGetResult(String[] p) {
        if (p.length < 2) return "ERROR|Missing jobId";
        String jobId = p[1];
 
        ReduceJob job = Reducer.getJob(jobId);
        if (job == null) return "ERROR|Unknown jobId: " + jobId;
 
        try {
            String result = job.waitForResult();
            Reducer.removeJob(jobId);
            return "RESULT|" + result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "ERROR|Interrupted";
        }
    }
}