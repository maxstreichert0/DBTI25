package fhwedel.JDBC;

import java.sql.*;

public class SchemaMigration {

    private static final String URL = "jdbc:mariadb://localhost:3306/firma";
    private static final String USER = "root";
    private static final String PASSWORD = "password";

    public static void main(String[] args) {
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
            createNewTables(conn);
            migrateData(conn);
            renameTables(conn);
            cleanupOldSchema(conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void createNewTables(Connection conn) throws SQLException {
        String createPersonalNeu = "CREATE TABLE personal_neu (" +
                                   "pnr INT PRIMARY KEY, " +
                                   "name CHAR(20) NOT NULL, " +
                                   "vorname CHAR(20), " +
                                   "geb_datum DATE, " +
                                   "kkid INT, " +
                                   "FOREIGN KEY (kkid) REFERENCES krankenversicherung(kkid))";
    
        String createKrankenkasse = "CREATE TABLE krankenversicherung (" +
                                    "kkid INT PRIMARY KEY, " +
                                    "kuerzel CHAR(3) NOT NULL, " +
                                    "name VARCHAR(100) NOT NULL)";
    
        String createPatent = "CREATE TABLE patent (" +
                              "patent_id INT PRIMARY KEY, " +
                              "pnr INT, " +
                              "FOREIGN KEY (pnr) REFERENCES personal_neu(pnr))";
    
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(createKrankenkasse);
            stmt.executeUpdate(createPersonalNeu);
            stmt.executeUpdate(createPatent);
            System.out.println("Neue Tabellen erfolgreich erstellt.");
    
            // Krankenkassen-Daten direkt einfügen
            String insertKrankenkasse = "INSERT INTO krankenversicherung (kkid, kuerzel, name) VALUES " +
                                        "(1, 'aok', 'Allgemeine Ortskrankenkasse'), " +
                                        "(2, 'bak', 'Betriebskrankenkasse B. Braun Aesculap'), " +
                                        "(3, 'bek', 'Barmer Ersatzkasse'), " +
                                        "(4, 'dak', 'Deutsche Angestelltenkrankenkasse'), " +
                                        "(5, 'tkk', 'Techniker Krankenkasse'), " +
                                        "(6, 'kkh', 'Kaufmännische Krankenkasse')";
    
            stmt.executeUpdate(insertKrankenkasse);
            System.out.println("Krankenkassen-Daten erfolgreich eingefügt.");
        }
    }
    

    // Schritt 2-4: Daten migrieren
    private static void migrateData(Connection conn) throws SQLException {
        // 1 Anzahl der alten Daten in personal abrufen
        String countOldSQL = "SELECT COUNT(*) FROM personal";
        int oldCount = 0;
    
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(countOldSQL)) {
            if (rs.next()) {
                oldCount = rs.getInt(1);
            }
        }
    
        if (oldCount == 0) {
            throw new SQLException("FEHLER: Die Tabelle 'personal' ist leer. Migration gestoppt.");
        }
    
        // 2️ Testen, ob `JOIN` korrekte Ergebnisse liefert
        String testJoinSQL = "SELECT COUNT(*) FROM personal p " +
                             "JOIN krankenversicherung k ON p.krankenkasse = k.kuerzel";
        int joinCount = 0;
    
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(testJoinSQL)) {
            if (rs.next()) {
                joinCount = rs.getInt(1);
            }
        }
    
        if (joinCount == 0) {
            throw new SQLException("FEHLER: Keine Datensätze für die Migration gefunden! Prüfe das JOIN.");
        }

        // 3️ Daten migrieren (nur, wenn der JOIN Werte liefert)
        String migrateSQL = "INSERT INTO personal_neu (pnr, name, vorname, kkid) " +
                            "SELECT p.pnr, p.name, p.vorname, k.kkid " +
                            "FROM personal p " +
                            "JOIN krankenversicherung k ON p.krankenkasse = k.kuerzel";
    
        int rowsMigrated = 0;
        try (Statement stmt = conn.createStatement()) {
            rowsMigrated = stmt.executeUpdate(migrateSQL);
        }
    
        // 4️ Sicherstellen, dass alle Daten migriert wurden
        String countNewSQL = "SELECT COUNT(*) FROM personal_neu";
        int newCount = 0;
    
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(countNewSQL)) {
            if (rs.next()) {
                newCount = rs.getInt(1);
            }
        }
    
        if (newCount == oldCount) {
            System.out.println("Alle Daten wurden erfolgreich migriert! Anzahl: " + rowsMigrated);
        } else {
            throw new SQLException("FEHLER: Datenverlust möglich! Erwartet: " + oldCount + ", Migriert: " + newCount);
        }
    }
    
    
    
    

    // Schritt 5: Tabellen umbenennen und Constraints anpassen
    private static void renameTables(Connection conn) throws SQLException {
        String renamePersonal = "RENAME TABLE personal TO personal_alt, personal_neu TO personal";
        String updatePatentFK = "ALTER TABLE patent DROP FOREIGN KEY patent_ibfk_1, " +
                                "ADD CONSTRAINT fk_personal FOREIGN KEY (pnr) REFERENCES personal(pnr)";

        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(renamePersonal);
            stmt.executeUpdate(updatePatentFK);
            System.out.println("Tabellen erfolgreich umbenannt und Constraints angepasst.");
        }
    }

    // Schritt 6-7: Altes Schema bereinigen
    private static void cleanupOldSchema(Connection conn) throws SQLException {
        String deleteOldData = "DELETE FROM personal_alt";
        String dropOldTable = "DROP TABLE personal_alt";

        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(deleteOldData);
            stmt.executeUpdate(dropOldTable);
            System.out.println("Altes Schema erfolgreich bereinigt.");
        }
    }
}
