package tzogos.user;

import android.widget.ArrayAdapter;
import android.widget.ListView;
import java.util.ArrayList;
import java.util.List;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ProgressBar;
import android.widget.TextView;
import android.widget.Toast;
import androidx.appcompat.app.AlertDialog;
import androidx.appcompat.app.AppCompatActivity;

public class MainActivity extends AppCompatActivity {

    private EditText editPlayerId, editRisk, editBetCategory, editStars;
    private EditText editGameName, editBetAmount, editBalanceAmount;
    private ProgressBar progressBar;
    private TcpClient tcpClient;

    private static final String MASTER_IP = "10.0.2.2";
    private static final int MASTER_PORT = 5000;

    // Inside MainActivity class
    private Button btnConnect, btnSearch, btnPlay, btnAddBalance;
    private TextView textStatus;
    private boolean isConnected = false;
    private ListView listGames;
    private ArrayAdapter<String> adapter;
    private List<String> displayedGameNames = new ArrayList<>();

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        tcpClient = new TcpClient(MASTER_IP, MASTER_PORT);

        // Bind new elements
        btnConnect = findViewById(R.id.btnConnect);
        btnSearch = findViewById(R.id.btnSearch);
        btnPlay = findViewById(R.id.btnPlay);
        btnAddBalance = findViewById(R.id.btnAddBalance);
        textStatus = findViewById(R.id.textStatus);

        // Bind existing inputs...
        editPlayerId = findViewById(R.id.editPlayerId);
        editRisk = findViewById(R.id.editRisk);
        editBetCategory = findViewById(R.id.editBetCategory);
        editStars = findViewById(R.id.editStars);
        editGameName = findViewById(R.id.editGameName);
        editBetAmount = findViewById(R.id.editBetAmount);
        editBalanceAmount = findViewById(R.id.editBalanceAmount);
        progressBar = findViewById(R.id.progressBar);

        // Set initial state: Disable everything except Connect

        listGames = findViewById(R.id.listGames);
        listGames.setNestedScrollingEnabled(true);
        // Αρχικοποίηση του Adapter
        adapter = new ArrayAdapter<>(this, android.R.layout.simple_list_item_1, displayedGameNames);
        listGames.setAdapter(adapter);

        // Τι συμβαίνει όταν ο χρήστης πατάει ένα παιχνίδι στη λίστα
        listGames.setOnItemClickListener((parent, view, position, id) -> {
            String selected = displayedGameNames.get(position);
            // Παίρνουμε μόνο το GameName αν το string είναι σύνθετο
            // Αν το string είναι "Roulette (Stars: 5)", καθαρίζουμε για το Play command
            String cleanName = selected.split(" \\(")[0];
            editGameName.setText(cleanName);
            Toast.makeText(this, "Selected: " + cleanName, Toast.LENGTH_SHORT).show();
        });
        setActionsEnabled(false);

        btnConnect.setOnClickListener(v -> handleConnect());
        btnSearch.setOnClickListener(v -> search());
        btnPlay.setOnClickListener(v -> play());
        btnAddBalance.setOnClickListener(v -> addBalance());
    }


    private void handleConnect() {
        String pId = editPlayerId.getText().toString().trim();
        if (pId.isEmpty()) {
            showPopup("Error", "Please enter a Player ID");
            return;
        }

        toggleLoading(true);

        // Κλήση της connect που φτιάξαμε παραπάνω
        tcpClient.connect(new TcpClient.Callback() {
            @Override
            public void onResponse(String response) {
                // Αν η σύνδεση πέτυχε, στέλνουμε ένα SEARCH για να βεβαιωθούμε ότι ο Master απαντάει
                tcpClient.sendRequest("SEARCH|*|*|*", new TcpClient.Callback() {
                    @Override
                    public void onResponse(String searchResp) {
                        toggleLoading(false);
                        isConnected = true;
                        setActionsEnabled(true);
                        textStatus.setText("Status: Connected as " + pId);
                        textStatus.setTextColor(android.graphics.Color.GREEN);
                        btnConnect.setText("Reconnect");
                        Toast.makeText(MainActivity.this, "Connection Established!", Toast.LENGTH_SHORT).show();
                    }

                    @Override
                    public void onError(String error) {
                        toggleLoading(false);
                        textStatus.setText("Status: Master unreachable");
                        showPopup("Error", "Master connected but didn't respond to search.");
                    }
                });
            }

            @Override
            public void onError(String error) {
                toggleLoading(false);
                textStatus.setText("Status: Connection Failed");
                showPopup("Connection Error", "Could not connect to " + MASTER_IP + "\n" + error);
            }
        });
    }

    private void setActionsEnabled(boolean enabled) {
        btnSearch.setEnabled(enabled);
        btnPlay.setEnabled(enabled);
        btnAddBalance.setEnabled(enabled);
        // Optional: Fade them out slightly if disabled
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

    private void updateGameList(String serverResponse) {
        displayedGameNames.clear();

        if (serverResponse == null || serverResponse.isEmpty() || serverResponse.equals("EMPTY")) {
            Toast.makeText(this, "No games found.", Toast.LENGTH_SHORT).show();
        } else {
            // Ο Master στέλνει: GameName=X,Stars=Y##GameName=Z,Stars=W
            String[] games = serverResponse.split("##");
            for (String gameData : games) {
                // Εξαγωγή του GameName για την εμφάνιση
                // Παράδειγμα gameData: "GameName=Poker,Provider=Amatic,Stars=4..."
                String name = "Unknown";
                String stars = "0";

                String[] parts = gameData.split(",");
                for(String p : parts) {
                    if(p.contains("GameName=")) name = p.split("=")[1];
                    if(p.contains("Stars=")) stars = p.split("=")[1];
                }

                displayedGameNames.add(name + " (★ " + stars + ")");
            }
        }

        adapter.notifyDataSetChanged(); // Ενημέρωση της οθόνης
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
                String title = response.contains("WIN") ? "🎉 YOU WON!" : "💸 Result";
                showPopup(title, response);
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
                // Using a Toast here because a full popup for balance is annoying
                Toast.makeText(MainActivity.this, "Balance Updated: " + response, Toast.LENGTH_LONG).show();
            }

            @Override
            public void onError(String error) {
                toggleLoading(false);
                showPopup("Error", error);
            }
        });
    }

    // --- UI HELPER METHODS ---

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