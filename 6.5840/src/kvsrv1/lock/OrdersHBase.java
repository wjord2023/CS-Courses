import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class OrdersHBase {
    private static final String NAMESPACE = "ns_sjy";
    private static final String TABLE_ORDERS = "orders_sjy";
    private static final String TABLE_USER = "user_sjy";

    public static void main(String[] args) throws IOException {
        // Step 1: Create HBase configuration and connection
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        Admin admin = connection.getAdmin();

        // Step 2: Create namespace if it doesn't exist
        try {
            NamespaceDescriptor ns = NamespaceDescriptor.create(NAMESPACE).build();
            admin.createNamespace(ns);
            System.out.println("Namespace created: " + NAMESPACE);
        } catch (NamespaceExistException e) {
            System.out.println("Namespace already exists: " + NAMESPACE);
        }

        // Step 3: Create orders_lx table with column family "info"
        TableName ordersTable = TableName.valueOf(NAMESPACE + ":" + TABLE_ORDERS);
        if (!admin.tableExists(ordersTable)) {
            TableDescriptor descriptor = TableDescriptorBuilder.newBuilder(ordersTable)
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of("info"))
                    .build();
            admin.createTable(descriptor);
            System.out.println("Table created: " + ordersTable.getNameAsString());
        }

        // Step 3.1: Create user_lx table with column family "base"
        TableName userTable = TableName.valueOf(NAMESPACE + ":" + TABLE_USER);
        if (!admin.tableExists(userTable)) {
            TableDescriptor descriptor = TableDescriptorBuilder.newBuilder(userTable)
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of("base"))
                    .build();
            admin.createTable(descriptor);
            System.out.println("Table created: " + userTable.getNameAsString());
        }

        // Step 4: Insert order data into orders_lx table
        Table table = connection.getTable(ordersTable);
        String[][] data = {
                {"OD250412001", "U25001", "item2301", "2", "50", "100", "Paid"},
                {"OD250412002", "U25002", "item2302", "1", "125", "125", "Delivered"},
                {"OD250412003", "U25003", "item2303", "3", "60", "180", "Finished"},
                {"OD250412004", "U25004", "item2301", "4", "45", "180", "Unpaid"},
                {"OD250412005", "U25005", "item2304", "1", "89", "89", "Paid"},
                {"OD250412006", "U25006", "item2305", "2", "30", "60", "Cancel"},
                {"OD250412007", "U25007", "item2302", "5", "60", "300", "Paid"},
        };

        for (String[] row : data) {
            Put put = new Put(Bytes.toBytes(row[0])); // RowKey
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("UserID"), Bytes.toBytes(row[1]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ProductID"), Bytes.toBytes(row[2]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("Count"), Bytes.toBytes(row[3]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("Price"), Bytes.toBytes(row[4]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("Total"), Bytes.toBytes(row[5]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("OrderState"), Bytes.toBytes(row[6]));
            table.put(put); // Insert into HBase
        }

        System.out.println("Data inserted into " + TABLE_ORDERS);

        // Step 5: Export all records to a local file
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        BufferedWriter writer = new BufferedWriter(new FileWriter("orders_output.txt"));
        for (Result result : scanner) {
            writer.write(result.toString()); // Write each record
            writer.newLine();
        }
        writer.close();
        System.out.println("Exported data to orders_output.txt");

        // Step 6: Delete record with OrderState = "Cancel"
        Get get = new Get(Bytes.toBytes("OD250412006"));
        Result result = table.get(get);
        String state = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("OrderState")));
        if ("Cancel".equals(state)) {
            Delete delete = new Delete(Bytes.toBytes("OD250412006"));
            table.delete(delete);
            System.out.println("Deleted canceled order OD250412006");
        }

        // Step 7: Add a new column family "family_lx" to user_lx table
        ColumnFamilyDescriptor newFamily = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("family_lx")).build();
        admin.modifyTable(TableDescriptorBuilder.newBuilder(userTable)
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("base")) // existing family
                .setColumnFamily(newFamily) // new family
                .build());
        System.out.println("Added new column family 'family_lx' to user_lx table");

        // Step 8: Delete user_lx table
        admin.disableTable(userTable);
        admin.deleteTable(userTable);
        System.out.println("Deleted user_lx table");

        // Cleanup
        table.close();
        connection.close();
    }
}
