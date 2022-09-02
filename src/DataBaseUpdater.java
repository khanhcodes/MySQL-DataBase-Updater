// Import SQL database and packages
import java.sql.PreparedStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.io.*;
import java.sql.ResultSet;

class DataBaseUpdater {
    public static void main(String args[]) {

        // Server login information
        String url = "jdbc:mysql://localhost:3306/";
        String username = "username";
        String password = "password";

        String csvFilePath = "resources/ItemPrice_Leadtime_fixed.csv";

        try {
            // Load driver class
            Class.forName("com.mysql.cj.jdbc.Driver");

            // Create connection object
            Connection con = DriverManager.getConnection(url, username, password);
            con.setAutoCommit(false);
            System.out.println("MySQL Connection Established!");

            // Create new database
            String sql1 = "CREATE DATABASE itemdb";
            PreparedStatement ps = con.prepareStatement(sql1);
            ps.execute(sql1);

            // Create a SQL table
            String sql2 = "CREATE TABLE itemdb.new_items" +
                    "(id INTEGER not NULL UNIQUE, " +
                    " name VARCHAR(255), " +
                    " description VARCHAR(255), " +
                    " cost DOUBLE, " +
                    " lead_time INTEGER)";
            PreparedStatement ps2 = con.prepareStatement(sql2);
            ps2.execute(sql2);

            // Create a SQL query to insert data into table and update cost and lead_time on
            // items with duplicate key
            String sql = "INSERT INTO itemdb.new_items (id, name, description, cost, lead_time) VALUES (?, ?, ?, ?, ?)"
                    +
                    " ON DUPLICATE KEY UPDATE cost = values(cost), lead_time = values(lead_time)";

            PreparedStatement statement = con.prepareStatement(sql);

            BufferedReader lineReader = new BufferedReader(new FileReader(csvFilePath));
            String lineText = null;

            // lineReader.readLine(); // Skip header line if CSV data has header

            while ((lineText = lineReader.readLine()) != null) {

                // Read data from CSV file
                String[] data = lineText.split(",");
                String id = data[0];
                String name = data[1];
                String description = data[2];
                String cost = data[6];
                String lead_time = data[5];

                // Parse data into SQL database
                statement.setInt(1, Integer.parseInt(id));
                statement.setString(2, name);
                statement.setString(3, description);
                statement.setFloat(4, Float.parseFloat(cost));
                statement.setInt(5, Integer.parseInt(lead_time));

                statement.addBatch();
            }

            lineReader.close();

            // Execute the remaining queries
            statement.executeBatch();

            ResultSet rs = statement.executeQuery("Select * from itemdb.new_items");
            System.out.println(
                    "id\t\tname\t\tdescription\t\tcost\t\tlead_time");
            System.out.println(
                    "-------------------------------------------------------");
            while (rs.next()) {
                System.out.println(rs.getString(1) + "\t\t"
                        + rs.getString(2)
                        + "\t\t"
                        + rs.getString(3)
                        + "\t\t"
                        + rs.getString(4)
                        + "\t\t"
                        + rs.getString(5));
            }

            con.commit();
            con.close();
            System.out.println("Data has been inserted successfully!");

        } catch (Exception e) {
            System.out.println(e);
        }
    }
}