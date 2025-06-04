package fhwedel.JDBC;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

public class CrudService {

    private static final String URL = "jdbc:mariadb://localhost:3306/firma";
    private static final String USER = "root";
    private static final String PASSWORD = "password";

    public static void main(String[] args) {
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
            insertPersonal(conn);
            updateGehalt(conn);
            selectPersonal(conn);
            deletePersonal(conn);
            selectEmployeesByDepartment(conn, "d15");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Neuen Eintrag hinzufügen
    private static void insertPersonal(Connection conn) throws SQLException {
        String sql = "INSERT INTO personal (PNR, Name, Vorname, Geh_Stufe, Abt_Nr, Krankenkasse) VALUES (417, 'Krause', 'Henrik', 'it1', 'd13', 'tkk')";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.executeUpdate();
            System.out.println("Neuer Eintrag erfolgreich hinzugefügt!");
        }
    }

    // Gehalt um 10% erhöhen
    private static void updateGehalt(Connection conn) throws SQLException {
        String sql = "UPDATE gehalt SET betrag = betrag * 1.1 WHERE geh_stufe = 'it1'";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.executeUpdate();
            System.out.println("Gehaltsstufe erfolgreich um 10% erhöht!");
        }
    }

    // Alle Personal-Einträge abrufen
    private static void selectPersonal(Connection conn) throws SQLException {
        String sql = "SELECT * FROM personal";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                System.out.println("ID: " + rs.getInt("PNR") + ", Name: " + rs.getString("Name"));
            }
        }
    }
    private static void deletePersonal(Connection conn) throws SQLException {
        String sql = "DELETE FROM personal WHERE Name = ? AND Vorname = ?";
    
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, "Tietze");
            pstmt.setString(2, "Lutz");
    
            int rowsDeleted = pstmt.executeUpdate();
            if (rowsDeleted > 0) {
                System.out.println("Lutz erfolgreich gelöscht!");
            } else {
                System.out.println("Kein Eintrag gefunden.");
            }
        }
    }
    private static void selectEmployeesByDepartment(Connection conn, String abtNr) throws SQLException {
        String sql = "SELECT PNR, Name, Vorname FROM personal WHERE Abt_Nr = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, abtNr);
            try (ResultSet rs = pstmt.executeQuery()) {
                System.out.println("Mitarbeiter in der Abteilung Verkauf:");
                while (rs.next()) {
                    System.out.println("ID: " + rs.getInt("PNR") + ", Name: " + rs.getString("Name") +
                                       ", Vorname: " + rs.getString("Vorname"));
                }
            }
        }
    }

    
}
