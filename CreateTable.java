package tn.insat.tp4;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
public class HelloHBase {
	private Table table1;
    public void createHbaseTable(String[] args) throws IOException {
      Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        Admin admin = connection.getAdmin();
        HTableDescriptor ht = new HTableDescriptor(TableName.valueOf(args[0]));

        for(int i=1;i<=args.length-1;i++){
            ht.addFamily(new HColumnDescriptor(args[i]));
        }
        
        System.out.println("Creating Table");
        createOrOverwrite(admin, ht);
        System.out.println("Done......");
    }
	public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }
    public static void main(String[] args) throws IOException {
        HelloHBase admin = new HelloHBase();
        admin.createHbaseTable(args);
    }
}