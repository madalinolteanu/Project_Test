package ro.teamnet.zth.appl;

import org.junit.Test;
import ro.teamnet.zth.api.em.EntityManager;
import ro.teamnet.zth.api.em.EntityManagerImpl;
import ro.teamnet.zth.appl.dao.DepartmentDao;
import ro.teamnet.zth.appl.domain.Department;
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
public class DepartmentDaoTest {
    @Test
    public void testFindById() throws SQLException, ClassNotFoundException {

        EntityManagerImpl entityManagerImpl = new EntityManagerImpl();
        DepartmentDao depDao = new DepartmentDao();

        Department departmentEnt;
        Department departmentDao;

        departmentEnt = entityManagerImpl.findById(Department.class, 10);
        departmentDao = depDao.findDepartmentbyId(10);

       // System.out.println(departmentDao.getId());
        assertEquals("They should be equal", departmentEnt.getId(), departmentDao.getId());



    }
    @Test
    public void testGetNextIdVal() throws SQLException, ClassNotFoundException {
        EntityManagerImpl entityManager = new EntityManagerImpl();
        DepartmentDao depDao = new DepartmentDao();

        int resultEM = entityManager.getNextIdVal("departments", "DEPARTMENT_ID");
        int resultDAO = depDao.getNextIdValforDepartment("DEPARTMENT_ID");
       // int resultActual = 207;
     //   assertEquals("WRONG !!!", resultActual, result);
        assertEquals("They should be equal",resultEM, resultDAO);
    }
    @Test
    public void testinsert() throws SQLException, ClassNotFoundException, NoSuchFieldException {
        Department depEM= new Department();
        Department depDao= new Department();
        EntityManagerImpl e = new EntityManagerImpl();
        DepartmentDao d = new DepartmentDao();
        EntityManager entity;
        depEM.setId(e.getNextIdVal("departments","DEPARTMENT_ID"));
        depDao.setId(d.getNextIdValforDepartment("DEPARTMENT_ID"));
        depEM.setDepartmentName("Sales");
        depDao.setDepartmentName("Sales");

        depEM.setLocations(1700);
        depDao.setLocations(1700);

        depEM = e.insert(depEM);
        depDao = d.insertDepartment(depDao);

        assertEquals("They should be equal", depEM.getId(), depEM.getId());

    }

    @Test
    public void testFindAll() throws SQLException, ClassNotFoundException {
        Department department = new Department();
        Department departmentDao= new Department();
        EntityManagerImpl e = new EntityManagerImpl();
        DepartmentDao dao= new DepartmentDao();
        List<Department> list = (List<Department>) e.findAll(department.getClass());
        List<Department> listdao = (List<Department>) dao.findAllDepartments();
        assertEquals("Size should be equal",list.size(), listdao.size());
    }
    @Test
    public void testFindByParams() throws SQLException, ClassNotFoundException, NoSuchFieldException {
        // Department myDepartment = new Department();
        EntityManagerImpl e = new EntityManagerImpl();
        DepartmentDao dao = new DepartmentDao();
        Map<String, Object> map = new HashMap<>();
        map.put("Department_id", 50);

        List<Department> list;

        list = e.findByParams(Department.class, map);
        List<Department> listdao = dao.findByParams(map);

      //  Department department = e.findById(Department.class, 50);
       // System.out.println("FIND BY ID :" + department.toString());
       // System.out.println("FIND BY PARAMS :" + list.get(0).toString());

      //  Integer idFindParams = list.get(0).getId();

        assertEquals("The size should be equal", list.size(), listdao.size());

    }
    @Test
    public void testUpdate() throws SQLException, ClassNotFoundException {
        Department dep = new Department();
        dep.setDepartmentName("Mama");
        dep.setId(10);
        dep.setLocations(1700);
        EntityManagerImpl e = new EntityManagerImpl();
        DepartmentDao dao = new DepartmentDao();
        List<Department> list;
        list = (List<Department>) e.update(dep);

      //  System.out.println(list.size());

        assertNotEquals("The size should be different ",list.size()+2,list.size());
    }


}
