package com.example.katanem;

import android.os.Bundle;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;
import android.widget.Toast;
import androidx.appcompat.app.AppCompatActivity;

public class MainActivity extends AppCompatActivity {

    private EditText editPlayerId, editRisk, editBetCategory, editStars;
    private EditText editGameName, editBetAmount, editBalanceAmount;
    private TextView textConsole;
    private TcpClient tcpClient;

    // Προεπιλεγμένη IP και Port του Master (αλλάξτε την IP με αυτή του υπολογιστή σας αν τρέχετε σε emulator)
    private static final String MASTER_IP = "10.0.2.2"; // Το 10.0.2.2 δείχνει στο localhost του host μηχανήματος στον Android Emulator
    private static final int MASTER_PORT = 5000;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        // Αρχικοποίηση TCP Client
        tcpClient = new TcpClient(MASTER_IP, MASTER_PORT);

        // Bind UI elements
        editPlayerId = findViewById(R.id.editPlayerId);
        editRisk = findViewById(R.id.editRisk);
        editBetCategory = findViewById(R.id.editBetCategory);
        editStars = findViewById(R.id.editStars);
        editGameName = findViewById(R.id.editGameName);
        editBetAmount = findViewById(R.id.editBetAmount);
        editBalanceAmount = findViewById(R.id.editBalanceAmount);
        textConsole = findViewById(R.id.textConsole);

        Button btnSearch = findViewById(R.id.btnSearch);
        Button btnPlay = findViewById(R.id.btnPlay);
        Button btnAddBalance = findViewById(R.id.btnAddBalance);

        btnSearch.setOnClickListener(v -> search());
        btnPlay.setOnClickListener(v -> play());
        btnAddBalance.setOnClickListener(v -> addBalance());
    }

    private void search() {
        String risk = getOrDefault(editRisk.getText().toString(), "*");
        String betCat = getOrDefault(editBetCategory.getText().toString(), "*");
        String stars = getOrDefault(editStars.getText().toString(), "*");

        String request = "SEARCH|" + risk + "|" + betCat + "|" + stars;
        logToConsole("Requesting: " + request);

        tcpClient.sendRequest(request, new TcpClient.Callback() {
            @Override
            public void onResponse(String response) {
                logToConsole("Search Results:\n" + response.replace("##", "\n"));
            }

            @Override
            public void onError(String error) {
                logToConsole("Error: " + error);
            }
        });
    }

    private void play() {
        String playerId = editPlayerId.getText().toString();
        String gameName = editGameName.getText().toString();
        String betAmount = editBetAmount.getText().toString();

        if (playerId.isEmpty() || gameName.isEmpty() || betAmount.isEmpty()) {
            Toast.makeText(this, "Please fill Player ID, Game Name, and Bet Amount", Toast.LENGTH_SHORT).show();
            return;
        }

        String request = "PLAY|" + playerId + "|" + gameName + "|" + betAmount;
        logToConsole("Playing: " + request);

        tcpClient.sendRequest(request, new TcpClient.Callback() {
            @Override
            public void onResponse(String response) {
                logToConsole("Play Result:\n" + response);
            }

            @Override
            public void onError(String error) {
                logToConsole("Error: " + error);
            }
        });
    }

    private void addBalance() {
        String playerId = editPlayerId.getText().toString();
        String amount = editBalanceAmount.getText().toString();

        if (playerId.isEmpty() || amount.isEmpty()) {
            Toast.makeText(this, "Please fill Player ID and Amount", Toast.LENGTH_SHORT).show();
            return;
        }

        String request = "ADD_BALANCE|" + playerId + "|" + amount;
        logToConsole("Adding Balance: " + request);

        tcpClient.sendRequest(request, new TcpClient.Callback() {
            @Override
            public void onResponse(String response) {
                logToConsole("Balance Updated:\n" + response);
            }

            @Override
            public void onError(String error) {
                logToConsole("Error: " + error);
            }
        });
    }

    private String getOrDefault(String value, String def) {
        return value.trim().isEmpty() ? def : value.trim();
    }

    private void logToConsole(String message) {
        textConsole.append("\n" + message);
        // Κρατάμε τα τελευταία logs για να μην γεμίσει η μνήμη
        if (textConsole.getText().length() > 5000) {
            textConsole.setText(message);
        }
    }
}