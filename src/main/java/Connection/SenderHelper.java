package Connection;

import Connection.SSHConnection;

import java.io.IOException;

public class SenderHelper {

    SSHConnection conn;
    public void onQuerySend(String query) throws Exception {
        System.out.println("sending query"+ query);
        conn.connect("java -jar D2_DB.jar R_DDR ");
    }


    public void onDumpSend(String dbName) throws IOException {
        System.out.println("sending dump request");

    }
}