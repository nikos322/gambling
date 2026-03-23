import java.io.*;
import java.net.*;
import java.util.*;

public class Reducer {
    private static final int DEFAULT_PORT = 6000;

    public static void main(String[] args) throws IOException{
        int port = args.length > 0 ? Integer.parseInt(args[0]): DEFAULT_PORT;

        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("[Reducer] Listening on port " + port);

        while (true) { 
            Socket masterSocket = serverSocket.accept();
            new Thread(new ReducerHandler(masterSocket)).start();
        }
    }
}

class ReducerHandler implements Runnable{
    private final Socket socket;

    public ReducerHandler(Socket socket) {
        this.socket = null;
    }

    @Override
    public void run() {
        try(
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream())
        ){
            String line;
            while ((line = in.readLine()) != null){
                String result = dispatch(line);
                out.println(result);
            }
        } catch(Exception e) {
            System.err.println("[ReducerHandler] "+ e.getMessage());
        } finally {
            try {socket.close();} catch (IOException ignored) {};
        }
    }
    private String dispatch(String msg){
        String[] parts = msg.split("\\|");
        return switch (parts[0]) {
            case "REDUCE_PROVIDER" -> reduceProvider(parts.length > 1? parts[1]: "");
            case "REDUCE_PLAYER" -> reducePlayer(parts.length > 1? parts[1]:"");
            default -> "ERROR|Unknown reduce command";
        };
    }
        
    private String reduceProvider(String mapResultsCombined){
        Map<String,Double> aggregated = new LinkedHashMap<>();

        String[] workerBlocks = mapResultsCombined.split(";");
        for (String block : workerBlocks) {
            if (block.isEmpty() || block.equals("EMPTY")) continue;
            String[] entries = block.split(",");
            for (String entry : entries){
                if (entry.isEmpty()) continue;
                String[] kv = entry.split("=");
                if (kv.length < 2) continue;
                String gameName = kv[0].trim();
                double value;
                try {value = Double.parseDouble(kv[1].trim());}
                catch (NumberFormatException e){continue;}
                aggregated.merge(gameName, value, Double::sum);
            }
        }

        if (aggregated.isEmpty()) return "No data found";

        StringBuilder sb = new StringBuilder();
        double total = 0;
        for (Map.Entry<String, Double> e: aggregated.entrySet()){
            double val = e.getValue();
            total += val;
            sb.append(e.getKey()).append(": ")
            .append(val >= 0? "+" : "-").append(String.format("%.2f", val))
            .append(" FUN, ");
        }
        sb.append("Total: ").append(total >= 0 ? "+" : "-").append(String.format("%.2f", total)).append(" FUN");
        return sb.toString();
    }

    private String reducePlayer(String mapResultsCombined){
        String playerId = null;
        double total = 0;
        String[] workerBlocks = mapResultsCombined.split(";");
        for (String block : workerBlocks) {
            if (block.isEmpty() || block.equals("EMPTY")) continue;
            String[] kv = block.split("=");
            if (kv.length < 2) continue;
            if (playerId == null) playerId = kv[0].trim();
            try { total += Double.parseDouble(kv[1].trim());}
            catch (NumberFormatException e) {}
        }
        if (playerId == null) return "No data found";
        return playerId + "Total Profit: "+ 
            (total >=0 ? "+" : "-" + String.format("%.2f", total) + " FUN");
    }

}