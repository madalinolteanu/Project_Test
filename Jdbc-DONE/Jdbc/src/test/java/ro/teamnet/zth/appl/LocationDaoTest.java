package ro.teamnet.zth.appl;

import org.junit.Test;
import ro.teamnet.zth.api.em.EntityManager;
import ro.teamnet.zth.api.em.EntityManagerImpl;
import ro.teamnet.zth.appl.dao.DepartmentDao;
import ro.teamnet.zth.appl.dao.LocationDao;
import ro.teamnet.zth.appl.domain.Department;
import ro.teamnet.zth.appl.domain.Location;
import ro.teamnet.zth.appl.domain.Job;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Created by user on 7/11/2016.
 */
public class LocationDaoTest {
    @Test
    public void testFindById() throws SQLException, ClassNotFoundException {

        EntityManagerImpl entityManagerImpl = new EntityManagerImpl();
        LocationDao depDao = new LocationDao();

        Location LocationEnt;
        Location LocationDao;

        LocationEnt = entityManagerImpl.findById(Location.class, 10);
        LocationDao = depDao.findLocationbyId(10);

        // System.out.println(LocationDao.getId());
        assertEquals("They should be equal", LocationEnt.getId(), LocationDao.getId());



    }
    @Test
    public void testGetNextIdVal() throws SQLException, ClassNotFoundException {
        EntityManagerImpl entityManager = new EntityManagerImpl();
        LocationDao depDao = new LocationDao();

        int resultEM = entityManager.getNextIdVal("Locations", "Location_ID");
        int resultDAO = depDao.getNextIdValforLocation("Location_ID");
        // int resultActual = 207;
        //   assertEquals("WRONG !!!", resultActual, result);
        assertEquals("They should be equal",resultEM, resultDAO);
    }
    @Test
    public void testinsert() throws SQLException, ClassNotFoundException, NoSuchFieldException {
        Location depEM= new Location();
        Location depDao= new Location();
        EntityManagerImpl e = new EntityManagerImpl();
        LocationDao d = new LocationDao();
        EntityManager entity;
        depEM.setId(e.getNextIdVal("Locations","Location_ID"));
        depDao.setId(d.getNextIdValforLocation("Location_ID"));
        depEM.setCity("Seattle");
        depDao.setCity("Seattle");
        depEM.setPostalCode("1112");
        depDao.setPostalCode("1112");
        depEM.setStateProvince("Romania");
        depDao.setStateProvince("Romania");
        depEM.setStreetAddress("vdfd");
        depDao.setStreetAddress("dedede");


        depEM = e.insert(depEM);
        depDao = d.insertLocation(depDao);

        assertEquals("They should be equal", depEM.getId(), depDao.getId());

    }
  /*  @Test
    public void testDelete() throws SQLException, ClassNotFoundException, NoSuchFieldException {
        Location Location = new Location();
        Location LocationDao = new Location();
        EntityManagerImpl e = new EntityManagerImpl();
        LocationDao dao = new LocationDao();
        List<Location> list = (List<Location>) e.findAll(Location.class);
        List<Location> listdao = (List<Location>) dao.findAllLocations();

        int firstSize = list.size();
        e.delete(Location);
        dao.


        assertNotEquals("The size should be different after delete call",firstSize,list2.size());
    }*/

    @Test
    public void testFindAll() throws SQLException, ClassNotFoundException {
        Location Location = new Location();
        Location LocationDao= new Location();
        EntityManagerImpl e = new EntityManagerImpl();
        LocationDao dao= new LocationDao();
        List<Location> list = (List<Location>) e.findAll(Location.getClass());
        List<Location> listdao = (List<Location>) dao.findAllLocations();
        assertEquals("Size should be equal",list.size(), listdao.size());
    }
    @Test
    public void testFindByParams() throws SQLException, ClassNotFoundException, NoSuchFieldException {
        // Department myDepartment = new Department();
        EntityManagerImpl e = new EntityManagerImpl();
        LocationDao dao = new LocationDao();
        Map<String, Object> map = new HashMap<>();
        map.put("Location_id", 50);

        List<Location> list;

        list = e.findByParams(Location.class, map);
        List<Location> listdao = dao.findByParams(map);


        assertEquals("The size should be equal", list.size(), listdao.size());

    }
}
