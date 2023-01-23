package analytics;
import java.io.*;
import java.util.*;
import logmanagement.LogManagement;
import queryimplementation.ParseQuery;
import static DDBMS.D2_DB.VIRTUAL_MACHINE;
import static queryimplementation.ParseQuery.DATABASES;

public class Analysis
{
    LogManagement logger = new LogManagement();

    public void countAllQueries(String username)
    {
        try
        {
            BufferedReader br = new BufferedReader(new FileReader(VIRTUAL_MACHINE + "/Logs/Query_Log.txt"));
            String line;
            List<String> databases = new ArrayList<>();
            while((line = br.readLine()) != null)
            {
                String[] lineSplit = line.split("\\|");
                if(lineSplit[1].trim().equals(username))
                {
                    databases.add(lineSplit[2].trim());
                }
            }
            br.close();
            if(databases.size() == 0)
            {
                System.out.println("There are no operations performed by " + username + "at this moment!");
            }
            else
            {
                BufferedWriter countQueries = new BufferedWriter(new FileWriter(VIRTUAL_MACHINE + "/Analytics/Number_Of_Queries.txt"));
                Map<String, Integer> hmap = new HashMap<>();
                for(String each : databases)
                {
                    if(hmap.containsKey(each))
                    {
                        int count = hmap.get(each);
                        hmap.put(each, count + 1);
                    }
                    else
                    {
                        hmap.put(each, 1);
                    }
                }
                countQueries.write("Analytics report for the user logged in for queries executed on the databases\n");
                countQueries.write("-----------------------------------------------------------------------------\n");
                System.out.println("Database Queries!");
                System.out.println("-----------------------------------------------------------------------------");
                for(Map.Entry each : hmap.entrySet())
                {
                    String input = "User " + username + " submitted " + each.getValue() + " queries for " +
                            each.getKey() + " running on " + VIRTUAL_MACHINE;
                    countQueries.write(input + "\n");
                    System.out.println(input);
                }
                countQueries.write("\n");
                countQueries.close();
                System.out.println();
                System.out.println();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }

    public void countUpdateQueries(String dbName)
    {
        try
        {
            BufferedReader br = new BufferedReader(new FileReader(VIRTUAL_MACHINE + "/Logs/Event_Log.txt"));
            String line;
            Map<String, Integer> operations = new HashMap<>();
            while((line = br.readLine()) != null)
            {
                String[] values = line.split("\\|");
                if(values.length == 5)
                {
                    if(values[2].trim().equals(dbName))
                    {
                        if(values[4].trim().toLowerCase(Locale.ROOT).startsWith("update"))
                        {
                            if(operations.containsKey(values[3]))
                            {
                                int count = operations.get(values[3]);
                                operations.put(values[3], count + 1);
                            }
                            else
                            {
                                operations.put(values[3], 1);
                            }
                        }
                    }
                }
            }
            br.close();
            if(operations.size() == 0)
            {
                System.out.println("There are no update operations performed on " + dbName + " table at this moment!");
            }
            else
            {
                BufferedWriter updateOperations = new BufferedWriter(new FileWriter(VIRTUAL_MACHINE + "/Analytics/Update_Operations.txt"));
                updateOperations.write("\nAnalytics report for update operations on the database " + dbName + "\n");
                updateOperations.write("-----------------------------------------------------------------------------\n");
                System.out.println("Update Operations!");
                System.out.println("-----------------------------------------------------------------------------");
                for(Map.Entry each : operations.entrySet())
                {
                    String input = "Total " + each.getValue() + " Update operations are performed on " + each.getKey();
                    updateOperations.write(input + "\n");
                    System.out.println(input);
                }
                updateOperations.write("\n");
                updateOperations.close();
                System.out.println();
                System.out.println();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }

    public static void main(String[] args)
    {
        ParseQuery pq = new ParseQuery();
        Analysis analysisObj = new Analysis();
        String name = args[0];

        System.out.println("Welcome to Analytics Module!");
        System.out.println("Please choose which operation to perform.");
        System.out.println("1. Count Total Queries");
        System.out.println("2. Count Number of Update Operations");

        Scanner sc = new Scanner(System.in);
        int choice = sc.nextInt();
        switch (choice)
        {
            case 1:
            {
                analysisObj.countAllQueries(name);
                break;
            }

            case 2:
            {
                System.out.println("Please enter the database name you want to count queries for: ");
                Scanner sc2 = new Scanner(System.in);
                String dbName = sc2.nextLine();
                try
                {
                    pq.getDatabase();
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
                boolean flag = false;
                for(String each : DATABASES)
                {
                    if(each.equals(dbName))
                    {
                        flag = true;
                        analysisObj.countUpdateQueries(each);
                    }
                }
                if(!flag)
                {
                    System.out.println("The database you entered is not available. Please try again!");
                }
                break;
            }

            default:
            {
                System.out.println("You entered an invalid input. Please try again!");
            }
        }
    }
}
