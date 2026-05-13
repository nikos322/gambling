package tzogos.user;

import android.os.Handler;
import android.os.Looper;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TcpClient {
    private final String serverIp;
    private final int serverPort;
    private final ExecutorService executor;
    private final Handler mainHandler;

    public interface Callback {
        void onResponse(String response);
        void onError(String error);
    }

    public TcpClient(String serverIp, int serverPort) {
        this.serverIp = serverIp;
        this.serverPort = serverPort;
        this.executor = Executors.newSingleThreadExecutor();
        this.mainHandler = new Handler(Looper.getMainLooper());
    }

    // Ασύγχρονη αποστολή μηνύματος και λήψη αποτελέσματος
    public void sendRequest(final String message, final Callback callback) {
        executor.execute(() -> {
            try (Socket socket = new Socket(serverIp, serverPort);
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                out.println(message);
                final String response = in.readLine();

                // Επιστροφή αποτελέσματος στο Main Thread
                mainHandler.post(() -> {
                    if (response != null) {
                        callback.onResponse(response);
                    } else {
                        callback.onError("No response from Master.");
                    }
                });

            } catch (Exception e) {
                mainHandler.post(() -> callback.onError("Connection Error: " + e.getMessage()));
            }
        });
    }
}