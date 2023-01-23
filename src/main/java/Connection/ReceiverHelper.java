package Connection;

import datadump.DataDumpHandler;
import queryimplementation.QueryImplementation;

import java.io.IOException;

public class ReceiverHelper {

    //query methods
    public void onQueryReceived(String query) throws IOException {
        System.out.println("onQueryReceived received query request"+ query);
        QueryImplementation qi=new QueryImplementation();
//        qi.execute(query);

    }

    public void onRegularQueryReceived() throws Exception {
        System.out.println("received regular query request");
        QueryImplementation qi=new QueryImplementation();
//        qi.regularExecute();

    }


    public void onDumpRecreived(String dbName) throws IOException {
        System.out.println("received dump request");
        DataDumpHandler ddh=new DataDumpHandler();
        ddh.exportDump(dbName);
    }
}
