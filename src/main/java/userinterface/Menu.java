package userinterface;
import analytics.Analysis;
import datadump.DataDumpHandler;
import datamodelling.DataModel;
import logmanagement.LogManagement;
import queryimplementation.QueryImplementation;
import java.util.Scanner;

public class Menu
{
    Registration register = new Registration();
    QueryImplementation query = new QueryImplementation();
    LogManagement logger = new LogManagement();
    DataModel dataModel = new DataModel();
    DataDumpHandler dump = new DataDumpHandler();

    public void mainMenu()
    {
        try
        {
            Login login = new Login();
            boolean shouldLoop = true;
            while(shouldLoop)
            {
                System.out.println();
                System.out.println("Please choose one of the following options:");
                System.out.println("1. REGISTER NEW USER");
                System.out.println("2. USER LOGIN");
                System.out.println("3. LOGOUT");
                Scanner sc = new Scanner(System.in);
                int option = sc.nextInt();

                switch (option)
                {
                    case 1:
                    {
                        register.registerUser();
                        break;
                    }

                    case 2:
                    {
                        login.loginUser();
                        break;
                    }

                    case 3:
                    {
                        System.out.println("Exiting the application...");
                        System.out.println("Thank you for using our DDBMS.D2_DB application..!");
                        System.exit(0);
                    }

                    default:
                    {
                        System.out.println("You entered an invalid input.");
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

    public void loginMenu(String userName) throws Exception
    {
        try
        {
            System.out.println("\nPlease choose one of the following options:");
            System.out.println("1. WRITE QUERIES");
            System.out.println("2. EXPORT SQL DUMP");
            System.out.println("3. DATA MODEL");
            System.out.println("4. ANALYTICS");
            System.out.println("5. SHOW LOGS");
            System.out.println("6. LOGOUT");
            System.out.println();
            System.out.print("Your choice : ");
            Scanner sc1 = new Scanner(System.in);
            int option = sc1.nextInt();

            switch (option)
            {
                case 1:
                {
                    String[] temp = {userName};
                    query.main(temp);
                    break;
                }

                case 2:
                {
                    System.out.println("Welcome to Export Dump Module");
                    System.out.println("Please enter a database for SQL Dump: ");
                    Scanner sc2 = new Scanner(System.in);
                    String database = sc2.nextLine();
                    dump.exportDump(database);
                    break;
                }

                case 3:
                {
                    String[] temp = {userName};
                    dataModel.main(temp);
                    break;
                }

                case 4:
                {
                    String[] temp = {userName};
                    Analysis.main(temp);
                    break;
                }

                case 5:
                {
                    String[] temp = {userName};
                    LogManagement.main(temp);
                    break;
                }

                case 6:
                {
                    System.out.println("Exiting the application...");
                    System.out.println("Thank you for using our D2_DB application..!");
                    System.exit(0);
                }

                default:
                {
                    System.out.println("You entered an invalid input.");
                    loginMenu(userName);
                }
            }

            System.out.println();
            System.out.println("Do you wish to stay on our application and perform more operations?");
            System.out.println("Type yes or no");
            Scanner sc2 = new Scanner(System.in);
            String ans = sc2.nextLine();

            if(ans.equalsIgnoreCase("yes"))
            {
                loginMenu(userName);
            }
            else if(ans.equalsIgnoreCase("no"))
            {
                System.out.println("Exiting the application...");
                System.out.println("Thank you for using our D2_DB application..!");
                System.exit(0);
            }
            else
            {
                System.out.println("Invalid entry. Please enter either yes or no to continue!");
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }
}
