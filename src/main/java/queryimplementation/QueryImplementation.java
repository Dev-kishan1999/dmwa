package queryimplementation;
import datadump.DataDumpHandler;
import java.io.IOException;
import java.util.*;
import logmanagement.LogManagement;
import static queryimplementation.ParseQuery.*;
import static queryimplementation.ExecuteQuery.*;
import static logmanagement.LogManagement.*;

public class QueryImplementation
{
    public static String DATABASE;
    public static String TABLE_NAME;

    public static boolean isTransaction = false;

    public static String LOCAL_METADATA_FILE = "Local_Meta_Data.txt";
    public static String GLOBAL_METADATA_FILE = "Global_Data_Dictionary.txt";

    public static List<String> DATABASES = new ArrayList<>();
    public static List<String> LOCAL_DATABASES = new ArrayList<>();
    public static List<String> GLOBAL_DATABASES = new ArrayList<>();


    public static void main(String[] args) throws Exception
    {
        LogManagement logger = new LogManagement();

        try
        {
            String userName = args[0];
            ParseQuery pq = new ParseQuery();
            ExecuteQuery eq = new ExecuteQuery();

            Scanner sc = new Scanner(System.in);
            System.out.println();
            System.out.println("Welcome to Query Implementation module!");
            System.out.println("Please enter a query to execute: ");

            boolean shouldLoop = true;
            while(shouldLoop)
            {
                long startTime, endTime, execTime;

                System.out.println("\nsql >");
                String query = sc.nextLine();
                if(query.toLowerCase(Locale.ROOT).equals("exit"))
                {
                    break;
                }
                else
                {
                    startTime=System.nanoTime();
                    if(pq.parseQuery(userName, query))
                    {
                        eq.executeQuery(userName, query);
                        endTime=System.nanoTime();
                        execTime=endTime-startTime;
                        if(DATABASE != null)
                        {
                            logger.generalLog(userName, DATABASE, execTime);
                            logger.queryLog(userName, DATABASE, query);
                        }
                    }
                    else
                    {
                        if (query.toLowerCase().contains("start")) {
                            System.out.println("Transaction Ended!");
                        }
                        else {
                            System.out.println("You entered an invalid query. Please enter a valid query.");
                            if(DATABASE != null)
                            {
                                logger.queryLog(userName, DATABASE, query);
                            }
                        }
                    }
                }
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }
}
