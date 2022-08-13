package com.sias.mapreduce.reduceJoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Edgar
 * @create 2022-08-12 9:13
 * @faction:
 */


/*一：定义序列化和反序列化方法，这样的话，就可以跨服务器运行相同的文件
 *    */
public class TableBean implements Writable {
    private String id; //订单id
    private String pid; //商品id
    private int amount; //商品数量
    private String pname;
    private String flag; //标记是那一张表 order pd

    public TableBean() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }


    /*1.序列化
     *   传递和保存数据，以及其他机器上使用和这个对象的时候
     *   以有序的子节流方式进行传递，能够保证数据的完整性*/
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeUTF(pid);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pname);
        dataOutput.writeUTF(flag);
    }

    /*2.反序列化
    *   在其他机器上，读取对象数据的话，先把数据从子节流的方式
    *   转换成以前数据的样子，在读，Input从子面上就可以知道
    *   把这个数据读进去
    *
    *   注意：序列化和反序列化顺序需要一致*/
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readUTF();
        this.pid = dataInput.readUTF();
        this.amount = dataInput.readInt();
        this.pname = dataInput.readUTF();
        this.flag = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return  id + "\t" + pname + "\t" + amount;
    }
}
