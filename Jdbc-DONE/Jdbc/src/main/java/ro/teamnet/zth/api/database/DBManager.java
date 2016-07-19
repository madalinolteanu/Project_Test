package ro.teamnet.zth.api.database;

import java.sql.*;

/**
 * Created by user on 7/8/2016.
 */
public class DBManager {
    private DBManager() {
        throw new UnsupportedOperationException();
    }
    private static final String CONNECTION_STRING ="jdbc:oracle:thin:@"+DBProperties.IP+":"+DBProperties.PORT;
    private static void registerDriver() throws ClassNotFoundException {
        try {
            Class.forName(DBProperties.DRIVER_CLASS);

        }
        catch (ClassNotFoundException err){
            System.out.println("Eroare Class Not Found");
            System.exit(-1);
        }
        }

    public static Connection getConnection() throws ClassNotFoundException, SQLException {
        registerDriver();
        return DriverManager.getConnection(CONNECTION_STRING, DBProperties.USER, DBProperties.PASS);

    }

    public static boolean checkConnection(Connection connection){

        try {
            Connection c2=getConnection();
            String SQL ="SELECT 1 FROM DUAL";
            Statement stmt = c2.createStatement();
            ResultSet res = stmt.executeQuery(SQL);
            if(res.next()==true){
                return true;
            }
            else
                return false;

        }
        catch (SQLException e){
            e.printStackTrace();
            System.exit(-1);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return false;
    }
}
