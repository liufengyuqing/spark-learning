package spark.sql;

import java.io.Serializable;

/**
 * create by liuzhiwei on 2020/5/12
 */
public class DeptBean implements Serializable {
    private int deptId;
    private String deptName;
    private String deptAddr;

    public int getDeptId() {
        return deptId;
    }

    public void setDeptId(int deptId) {
        this.deptId = deptId;
    }

    public String getDeptName() {
        return deptName;
    }

    public void setDeptName(String deptName) {
        this.deptName = deptName;
    }

    public String getDeptAddr() {
        return deptAddr;
    }

    public void setDeptAddr(String deptAddr) {
        this.deptAddr = deptAddr;
    }
}
