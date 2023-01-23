package DDBMS;
import logmanagement.LogManagement;
import userinterface.*;
import java.io.File;

public class D2_DB
{
    public static String VIRTUAL_MACHINE = "VM2";

    public static String LOCAL_METADATA_FILE = "Local_Meta_Data.txt";
    public static String GLOBAL_METADATA_FILE = "Global_Data_Dictionary.txt";
    public static String USER_PROFILE = "User_Profile.txt";
    public static String BASE_DIRECTORY = VIRTUAL_MACHINE + "/";
    public static String EVENT_LOG = "Event_Log.txt";
    public static String QUERY_LOG = "Query_Log.txt";
    public static String GENERAL_LOG = "General_Log.txt";
    LogManagement logger = new LogManagement();

    public void checkRootDirectory()
    {
        try
        {
            File root_directory = new File(BASE_DIRECTORY);
            if(!root_directory.exists())
            {
                root_directory.mkdirs();
                checkLogFolder();
                checkDataModelFolder();
                checkSQLDump();
                checkAnalytics();
            }
            checkMetadataFile();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }

    public void checkMetadataFile()
    {
        try
        {
            File local_metadata = new File(BASE_DIRECTORY + LOCAL_METADATA_FILE);
            File global_metadata = new File(BASE_DIRECTORY + GLOBAL_METADATA_FILE);
            File user_profile = new File(BASE_DIRECTORY + USER_PROFILE);

            if (!local_metadata.exists())
            {
                local_metadata.createNewFile();
            }

            if (!global_metadata.exists())
            {
                global_metadata.createNewFile();
            }

            if (!user_profile.exists())
            {
                user_profile.createNewFile();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }

    public void checkLogFolder()
    {
        try
        {
            File root_directory = new File(BASE_DIRECTORY + "Logs/");
            File query_log = new File(VIRTUAL_MACHINE + "/Logs/" + QUERY_LOG);
            File event_log = new File(VIRTUAL_MACHINE + "/Logs/" + EVENT_LOG);
            File general_log = new File(VIRTUAL_MACHINE + "/Logs/" + GENERAL_LOG);

            if(!root_directory.exists())
            {
                root_directory.mkdirs();
                if(!query_log.exists())
                {
                    query_log.createNewFile();
                }
                if(!event_log.exists())
                {
                    event_log.createNewFile();
                }
                if(!general_log.exists())
                {
                    general_log.createNewFile();
                }
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }

    public void checkDataModelFolder()
    {
        try
        {
            File root_directory = new File(BASE_DIRECTORY + "DataModelling/");
            if(!root_directory.exists())
            {
                root_directory.mkdirs();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }

    public void checkSQLDump()
    {
        try
        {
            File root_directory = new File(BASE_DIRECTORY + "SQLDump/");
            if(!root_directory.exists())
            {
                root_directory.mkdirs();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }

    public void checkAnalytics()
    {
        try
        {
            File root_directory = new File(BASE_DIRECTORY + "Analytics/");
            if(!root_directory.exists())
            {
                root_directory.mkdirs();
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
        D2_DB d2db = new D2_DB();
        Menu menu = new Menu();
        System.out.println();
        System.out.println("Welcome to D2_DB application..!");
        d2db.checkRootDirectory();
        menu.mainMenu();
    }
}
