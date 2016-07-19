package ro.teamnet.zth.api.database;

import org.junit.Test;
import ro.teamnet.zth.api.em.EntityUtils;
import ro.teamnet.zth.appl.domain.Department;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

/**
 * Created by user on 7/8/2016.
 */
public class DBManagerTest {

    @Test
    public void testgetConnection() {
        try {
            Connection c = DBManager.getConnection();
            boolean connection= false;
            if(c!=null)
                connection=true;
            assertEquals("The connection should be done", true, connection);

        }
        catch (ClassNotFoundException ex){
            System.out.println("Eroare Class");
            System.exit(-1);

        }
        catch (SQLException exc){
            System.out.println("Eroare Sql");
            System.exit(-1);
        }
      //
    }

    @Test
    public void testcheckConnection() {
        try {
            Connection c = DBManager.getConnection();
            boolean connected = DBManager.checkConnection(c);

            assertEquals("The connection should be done", true,connected );

        }
        catch (ClassNotFoundException ex){
            System.out.println("Eroare Class");
            System.exit(-1);

        }
        catch (SQLException exc){
            System.out.println("Eroare Sql");
            System.exit(-1);
        }
    }

}
