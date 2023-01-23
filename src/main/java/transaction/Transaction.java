package transaction;
import logmanagement.LogManagement;
import queryimplementation.ExecuteQuery;
import queryimplementation.ParseQuery;
import java.util.*;
import static queryimplementation.QueryImplementation.isTransaction;

public class Transaction
{
    //For storing query validation and type this kind:--- true;DDL
    public static Map<String, String> queryTable = new LinkedHashMap<>();
    public static long startTime, endTime;
    LogManagement logger = new LogManagement();
    public static boolean checker = true;

    public void startTransaction(String userName)
    {
        isTransaction = true;
        try
        {
            startTime = System.currentTimeMillis();
            Transaction transaction = new Transaction();
            String id = Config.idGenerator();
            logger.transactionLog("Transaction started!");
            transaction.takeInputQuery(userName, id);
            System.out.println("Total time taken for transaction :"+ (endTime-startTime)/1000 + " sec.\n" );
            logger.transactionTime((endTime-startTime)/1000);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }

    //Taking input in transaction module
    public void takeInputQuery(String userName, String transactionId)
    {
        try
        {
            LockManager lock = new LockManager();
            Transaction transaction = new Transaction();
            ParseQuery parse = new ParseQuery();
            ExecuteQuery execute = new ExecuteQuery();

            boolean flag = true;
            Scanner sc = new Scanner(System.in);
            while(flag){
                System.out.println("\ntransaction >");
                String query = sc.nextLine();
                String[] queryWords = query.split("\\s+");
                if(query.equalsIgnoreCase("commit;") || query.equalsIgnoreCase("rollback;")){
                    //execution
                    transaction.startExecution(userName, query);
                    break;
                } else if(queryWords[0].equalsIgnoreCase("use")){
                    // skip if use [DatabaseName] is inserted.
                    parse.parseQuery(userName, query);
                    execute.executeQuery(userName, query);
                    String database = queryWords[1];
                } else if(queryWords[0].equalsIgnoreCase("create") && queryWords[1].equalsIgnoreCase("table")){
                    boolean personalChecker;
                    if(checker){
                        personalChecker=parse.parseQuery(userName, query);
                        if(personalChecker){
                            execute.executeQuery(userName, query);
                        } else {
                            System.out.println("Syntactical Error!");
                        }
                    }
                } else if(queryWords[0].equalsIgnoreCase("create") && queryWords[1].equalsIgnoreCase("database")){
                    if(checker){
                        parse.parseQuery(userName, query);
                        execute.executeQuery(userName, query);
                    }
                } else {
                    transaction.makeHashmap(userName, query,transactionId);
                }
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }

    private void startExecution(String userName, String endQuery)
    {
        try
        {
            String[] keys = queryTable.keySet().toArray(new String[0]);
            ArrayList<String> ddlQuery = new ArrayList<String>();
            ExecuteQuery execute = new ExecuteQuery();
            LockManager lock = new LockManager();

            boolean flag = false;
            for(String key : keys){
                String value = queryTable.get(key);
                String[] results = value.split(";");
                if(results[1].equalsIgnoreCase("ddl")){
                    ddlQuery.add(key);
                }
                if(results[0].equalsIgnoreCase("false")){
                    flag = true;
                    break;
                }
            }
            if(flag || endQuery.equalsIgnoreCase("rollback;")){
                for(String query : ddlQuery){
                    execute.executeQuery(userName, query);
                }
            } else {
                for(String query : keys){
                    execute.executeQuery(userName, query);
                }
            }
            lock.releaseLock();
            endTime = System.currentTimeMillis();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }

    private void makeHashmap(String userName, String query, String transactionId)
    {
        try {
            ParseQuery parse = new ParseQuery();
            LockManager lock = new LockManager();

            boolean validation = parse.parseQuery(userName, query);
            if(!validation){
                checker = false;
            }
            boolean locked = lock.acquireLock(query, transactionId);

            if (locked) {
                String[] words = query.split("\\s+");
                if (Arrays.asList(Config.DDL).contains(words[0].toLowerCase())) {
                    if (validation) {
                        queryTable.put(query, "true;DDL");
                    } else {
                        queryTable.put(query, "false;DDL");
                    }
                } else if (Arrays.asList(Config.DML).contains(words[0].toLowerCase())) {
                    if (validation) {
                        queryTable.put(query, "true;DML");
                    } else {
                        queryTable.put(query, "false;DML");
                    }
                } else {
                    //nothing
                }
            } else {
                System.out.println(" Table is locked by another transaction.");
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }
}
