package ro.teamnet.zth.appl.dao;

import ro.teamnet.zth.api.em.EntityManager;
import ro.teamnet.zth.api.em.EntityManagerImpl;
import ro.teamnet.zth.appl.domain.Location;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static javafx.scene.input.KeyCode.T;

/**
 * Created by user on 7/11/2016.
 */
public class LocationDao {

    public EntityManager manager= new EntityManagerImpl();

    public LocationDao() throws SQLException, ClassNotFoundException {
    }

    public Location findLocationbyId(int id){

        return manager.findById(Location.class,id);

    }

    public int getNextIdValforLocation(String columnName) throws SQLException, ClassNotFoundException {
        int result = manager.getNextIdVal("Locations",columnName);

        return  result;

    }

    public Location insertLocation(Location dep) throws SQLException, NoSuchFieldException, ClassNotFoundException {
        Object result = manager.insert(dep);
        if(result.getClass().equals(Location.class))
            return (Location)result;
        else
            return null;
    }

    public List<Location> findAllLocations() throws SQLException {
        List<Location> result= manager.findAll(Location.class);

        return  result;

    }


    public List<Location> findByParams(Map<String,Object> map) throws SQLException, ClassNotFoundException, NoSuchFieldException {
        EntityManagerImpl em = new EntityManagerImpl();
        return em.findByParams(Location.class, map);
    }
}
