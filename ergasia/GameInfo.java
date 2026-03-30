public class GameInfo {
    String gameName;
    String providerName;
    int stars;
    int noOfVotes;
    String gameLogo;
    double minBet;
    double maxBet;
    String riskLevel;
    String hashKey;
    boolean active = true;

    String betCategory;
    double jackpot;

    public GameInfo(String gameName,
                    String providerName,
                    int stars,
                    int noOfVotes,
                    String gameLogo,
                    double minBet,
                    double maxBet,
                    String riskLevel,
                    String hashKey) {
        this.gameName = gameName;
        this.providerName = providerName;
        this.stars = stars;
        this.noOfVotes = noOfVotes;
        this.gameLogo = gameLogo;
        this.minBet = minBet;
        this.maxBet = maxBet;
        this.riskLevel = riskLevel;
        this.hashKey = hashKey;

        this.betCategory = computeBetCategory(minBet);
        this.jackpot = computeJackpot(riskLevel);
    }

    private String computeBetCategory(double minBet) {
        if (minBet >= 5.0) return "$$$";
        if (minBet >= 1.0) return "$$";
        return "$";
    }

    private double computeJackpot(String riskLevel) {
        if (riskLevel == null) return 10.0;
        return switch (riskLevel.toLowerCase()) {
            case "low" -> 10.0;
            case "medium" -> 20.0;
            case "high" -> 40.0;
            default -> 10.0;
        };
    }

    public boolean matchesFilter(String risk, String bet, int minStars) {
        if (!active) return false;

        boolean riskOk = risk.equals("*") || risk.equalsIgnoreCase(riskLevel);
        boolean betOk = bet.equals("*") || bet.equals(betCategory);
        boolean starsOk = stars >= minStars;

        return riskOk && betOk && starsOk;
    }

    @Override
    public String toString() {
        return "GameName=" + gameName
                + ",ProviderName=" + providerName
                + ",Stars=" + stars
                + ",MinBet=" + minBet
                + ",MaxBet=" + maxBet
                + ",RiskLevel=" + riskLevel
                + ",BetCategory=" + betCategory
                + ",Jackpot=" + jackpot;
    }
}