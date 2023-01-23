package Connection;

public class EntryPoint {

    private static final String DDB = "DDB";
    private static final String R_DDB = "R_DDB";

    public static String TABLE_NAME;

    static ReceiverHelper rh;
    static SenderHelper sh;

    public static void main(String[] args) throws Exception {

        rh=new ReceiverHelper();
        sh=new SenderHelper();
        String entry=args[0];
        System.out.println("entered main");


        if(entry.equals(DDB)){

            System.out.println("entered regular query DDB");
            sh.onQuerySend("CREATE DATABASE students;");
            //rh.onRegularQueryReceived();
        }else{
            System.out.println("entered special query R_DDB");
            String query=args[1];
            System.out.println("query is "+query);

            rh.onQueryReceived(query);
        }
    }
}
