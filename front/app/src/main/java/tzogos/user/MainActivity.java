package tzogos.user;

import android.os.Bundle;
import android.view.View;
import android.widget.ArrayAdapter;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ListView;
import android.widget.ProgressBar;
import android.widget.TextView;
import android.widget.Toast;
import androidx.appcompat.app.AlertDialog;
import androidx.appcompat.app.AppCompatActivity;
import java.util.ArrayList;
import java.util.List;

public class MainActivity extends AppCompatActivity {

    // Connection configuration fields
    private EditText editMasterIp, editMasterPort, editPlayerId;
    private EditText editRisk, editBetCategory, editStars, editGameName, editBetAmount, editBalanceAmount;
    private Button btnConnect, btnSearch, btnPlay, btnAddBalance;
    private TextView textStatus;
    private ProgressBar progressBar;
    private TcpClient tcpClient;
    private boolean isConnected = false;
    private ListView listGames;
    private ArrayAdapter<String> adapter;
    private List<String> displayedGameNames = new ArrayList<>();

    // Other gameplay fields
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        // Bind connection inputs
        editMasterIp = findViewById(R.id.editMasterIp);
        editMasterPort = findViewById(R.id.editMasterPort);
        editPlayerId = findViewById(R.id.editPlayerId);
        btnConnect = findViewById(R.id.btnConnect);
        textStatus = findViewById(R.id.textStatus);
        progressBar = findViewById(R.id.progressBar);

        // Bind gameplay inputs
        editRisk = findViewById(R.id.editRisk);
        editBetCategory = findViewById(R.id.editBetCategory);
        editStars = findViewById(R.id.editStars);
        editGameName = findViewById(R.id.editGameName);
        editBetAmount = findViewById(R.id.editBetAmount);
        editBalanceAmount = findViewById(R.id.editBalanceAmount);

        btnSearch = findViewById(R.id.btnSearch);
        btnPlay = findViewById(R.id.btnPlay);
        btnAddBalance = findViewById(R.id.btnAddBalance);

        // Setup ListView
        listGames = findViewById(R.id.listGames);
        listGames.setNestedScrollingEnabled(true);
        adapter = new ArrayAdapter<>(this, android.R.layout.simple_list_item_1, displayedGameNames);
        listGames.setAdapter(adapter);

        listGames.setOnItemClickListener((parent, view, position, id) -> {
            String selected = displayedGameNames.get(position);
            String cleanName = selected.split(" \\(")[0];
            editGameName.setText(cleanName);
        });

        // Lock interface until a successful connection happens
        setActionsEnabled(false);

        btnConnect.setOnClickListener(v -> handleConnect());
        btnSearch.setOnClickListener(v -> search());
        btnPlay.setOnClickListener(v -> play());
        btnAddBalance.setOnClickListener(v -> addBalance());
    }

    private void handleConnect() {
        String ip = editMasterIp.getText().toString().trim();
        String portString = editMasterPort.getText().toString().trim();
        String pId = editPlayerId.getText().toString().trim();

        if (ip.isEmpty() || portString.isEmpty() || pId.isEmpty()) {
            showPopup("Error", "Please input IP, Port, and a valid Player ID!");
            return;
        }

        int port;
        try {
            port = Integer.parseInt(portString);
        } catch (NumberFormatException e) {
            showPopup("Error", "Port must be a valid number.");
            return;
        }

        toggleLoading(true);
        textStatus.setText("Status: Connecting...");

        // Initialize the TCP client to establish a targeted connection with the provided IP and Port
        tcpClient = new TcpClient(ip, port);

        tcpClient.connect(new TcpClient.Callback() {
            @Override
            public void onResponse(String response) {
                // Send an initial request to verify the server is responding
                tcpClient.sendRequest("SEARCH|*|*|*", new TcpClient.Callback() {
                    @Override
                    public void onResponse(String searchResp) {
                        toggleLoading(false);
                        isConnected = true;
                        setActionsEnabled(true);
                        textStatus.setText("Connected to " + ip + ":" + port + " as " + pId);
                        textStatus.setTextColor(android.graphics.Color.parseColor("#4CAF50"));
                        btnConnect.setText("Reconnect");
                        Toast.makeText(MainActivity.this, "Online!", Toast.LENGTH_SHORT).show();
                    }

                    @Override
                    public void onError(String error) {
                        toggleLoading(false);
                        textStatus.setText("Status: Master Unreachable");
                        showPopup("Error", "Socket opened, but Master node didn't respond to requests.");
                    }
                });
            }

            @Override
            public void onError(String error) {
                toggleLoading(false);
                textStatus.setText("Status: Connection Failed");
                textStatus.setTextColor(android.graphics.Color.RED);
                showPopup("Network Error", "Could not create socket at " + ip + ":" + port + "\n" + error);
            }
        });
    }
    //Enables buttons/actions when app opens and connection is established
    private void setActionsEnabled(boolean enabled) {
        btnSearch.setEnabled(enabled);
        btnPlay.setEnabled(enabled);
        btnAddBalance.setEnabled(enabled);
        float alpha = enabled ? 1.0f : 0.5f;
        btnSearch.setAlpha(alpha);
        btnPlay.setAlpha(alpha);
        btnAddBalance.setAlpha(alpha);
    }

    private void search() {
        String risk = getOrDefault(editRisk.getText().toString(), "*");
        String betCat = getOrDefault(editBetCategory.getText().toString(), "*");
        String stars = getOrDefault(editStars.getText().toString(), "*");

        String request = "SEARCH|" + risk + "|" + betCat + "|" + stars;

        toggleLoading(true);
        tcpClient.sendRequest(request, new TcpClient.Callback() {
            @Override
            public void onResponse(String response) {
                toggleLoading(false);
                updateGameList(response);
            }

            @Override
            public void onError(String error) {
                toggleLoading(false);
                showPopup("Error", error);
            }
        });
    }

    //Lists the games that match the search filters
    private void updateGameList(String serverResponse) {
        displayedGameNames.clear();

        if (serverResponse == null || serverResponse.isEmpty() || serverResponse.equals("EMPTY")) {
            Toast.makeText(this, "No games found.", Toast.LENGTH_SHORT).show();
        } else {
            String[] games = serverResponse.split("##");
            for (String gameData : games) {
                String name = "Unknown";
                String stars = "0";
                String min = "0";
                String max = "1000";
                String[] parts = gameData.split(",");
                for(String p : parts) {
                    if(p.contains("GameName=")) name = p.split("=")[1];
                    if(p.contains("Stars=")) stars = p.split("=")[1];
                    if(p.contains("MinBet=")) min = p.split("=")[1];
                    if(p.contains("MaxBet=")) max = p.split("=")[1];
                }

                displayedGameNames.add(name + " (★ " + stars + ")\n" + min + "/" +max);
            }
        }

        adapter.notifyDataSetChanged();
    }

    private void play() {
        String pId = editPlayerId.getText().toString();
        String gName = editGameName.getText().toString();
        String bet = editBetAmount.getText().toString();

        if (pId.isEmpty() || gName.isEmpty() || bet.isEmpty()) {
            Toast.makeText(this, "Fill in all play details!", Toast.LENGTH_SHORT).show();
            return;
        }

        toggleLoading(true);
        tcpClient.sendRequest("PLAY|" + pId + "|" + gName + "|" + bet, new TcpClient.Callback() {
            @Override
            public void onResponse(String response) {
                toggleLoading(false);
                // Customizing the win/loss message
                String title = response.contains("WIN") || response.contains("JACKPOT")? "🎉 YOU WON!" : "💸 You Lost";
                String message = "";
                if (response.split("\\|").length < 3) {
                    message = "Not Enough Funds/Wrong Bet Amount";
                    title = "Error";
                } else {
                    message = "Winnings: " + response.split("\\|")[2].split("=")[1] +"\nBalance: " +
                            response.split("\\|")[3].split("=")[1];
                }
                showPopup(title, message);
            }

            @Override
            public void onError(String error) {
                toggleLoading(false);
                showPopup("Game Error", error);
            }
        });
    }

    private void addBalance() {
        String pId = editPlayerId.getText().toString();
        String amount = editBalanceAmount.getText().toString();

        if (pId.isEmpty() || amount.isEmpty()) return;

        toggleLoading(true);
        tcpClient.sendRequest("ADD_BALANCE|" + pId + "|" + amount, new TcpClient.Callback() {
            @Override
            public void onResponse(String response) {
                toggleLoading(false);
                //message for addbalance
                Toast.makeText(MainActivity.this,response.contains("OK") ?
                        "Balance Updated: Total: " + response.split("\\|")[1].split("=")[1] +"$" : "Error!!Balance not Updated" , Toast.LENGTH_LONG).show();
            }

            @Override
            public void onError(String error) {
                toggleLoading(false);
                showPopup("Error", error);
            }
        });
    }

    private void toggleLoading(boolean isLoading) {
        progressBar.setVisibility(isLoading ? View.VISIBLE : View.GONE);
    }

    private void showPopup(String title, String message) {
        new AlertDialog.Builder(this)
                .setTitle(title)
                .setMessage(message)
                .setPositiveButton("OK", null)
                .show();
    }

    private String getOrDefault(String value, String def) {
        return value.trim().isEmpty() ? def : value.trim();
    }
}