package stu.liuhl.mr.reducejoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReduceBean implements Writable {
    private String tid;
    private String pid;
    private int amount;
    private String pname;
    private String flag;

    public ReduceBean() {
    }

    public String getTid() {
        return tid;
    }

    public void setTid(String tid) {
        this.tid = tid;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.tid);
        dataOutput.writeUTF(this.pid);
        dataOutput.writeInt(this.amount);
        dataOutput.writeUTF(this.pname);
        dataOutput.writeUTF(this.flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.tid = dataInput.readUTF();
        this.pid = dataInput.readUTF();
        this.amount = dataInput.readInt();
        this.pname = dataInput.readUTF();
        this.flag = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return tid + "\t" +
                pname + "\t" +
                amount + "\n";
    }
}
