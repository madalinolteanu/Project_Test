package ro.teamnet.zth.appl.dao;

import ro.teamnet.zth.api.em.EntityManager;
import ro.teamnet.zth.api.em.EntityManagerImpl;
import ro.teamnet.zth.appl.domain.Department;
import ro.teamnet.zth.appl.domain.Location;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static javafx.scene.input.KeyCode.T;

/**
 * Created by user on 7/11/2016.
 */
public class DepartmentDao {

    public EntityManager manager= new EntityManagerImpl();

    public DepartmentDao() throws SQLException, ClassNotFoundException {
    }

    public Department findDepartmentbyId(int id){

        return manager.findById(Department.class,id);

    }

    public int getNextIdValforDepartment(String columnName) throws SQLException, ClassNotFoundException {
        int result = manager.getNextIdVal("Departments",columnName);

        return  result;

    }

    public Department insertDepartment(Department dep) throws SQLException, NoSuchFieldException, ClassNotFoundException {
        Object result = manager.insert(dep);
        if(result.getClass().equals(Department.class))
            return (Department)result;
        else
            return null;
    }

    public List<Department> findAllDepartments() throws SQLException {
        List<Department> result= manager.findAll(Department.class);

        return  result;

    }

    public List<Department> findByParams(Map<String,Object> map) throws SQLException, ClassNotFoundException, NoSuchFieldException {
        EntityManagerImpl em = new EntityManagerImpl();
        return em.findByParams(Department.class, map);
    }

    public Department updateDepartment(Department dep){
        return manager.update(dep);
    }
}
