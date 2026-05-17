package tzogos.user;

import android.os.Handler;
import android.os.Looper;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TcpClient {
    private final String serverIp;
    private final int serverPort;
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;
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

    public void connect(final Callback callback) {
        executor.execute(() -> {
            try {
                if (socket != null) socket.close();

                socket = new Socket();
                socket.connect(new InetSocketAddress(serverIp, serverPort));

                out = new PrintWriter(socket.getOutputStream(), true);
                in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                mainHandler.post(() -> callback.onResponse("CONNECTED"));
            } catch (Exception e) {
                mainHandler.post(() -> callback.onError("Failed to connect: " + e.getMessage()));
            }
        });
    }

    public void sendRequest(final String message, final Callback callback) {
        executor.execute(() -> {
            try {
                if (socket == null || socket.isClosed() || out == null) {
                    mainHandler.post(() -> callback.onError("Not connected to server"));
                    return;
                }

                out.println(message);
                final String response = in.readLine();

                mainHandler.post(() -> {
                    if (response != null) {
                        callback.onResponse(response);
                    } else {
                        callback.onError("No response from server.");
                    }
                });
            } catch (Exception e) {
                mainHandler.post(() -> callback.onError("Communication error: " + e.getMessage()));
            }
        });
    }

    public void disconnect() {
        executor.execute(() -> {
            try {
                if (socket != null) socket.close();
            } catch (Exception ignored) {}
        });
    }
}