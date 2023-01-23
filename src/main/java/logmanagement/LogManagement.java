package logmanagement;
import java.io.*;
import java.time.LocalDate;
import java.time.LocalTime;
import static DDBMS.D2_DB.VIRTUAL_MACHINE;

public class LogManagement
{
    public String getTime()
    {
        String dBTime = LocalDate.now() + " " + LocalTime.now();
        return dBTime;
    }




    public void queryLog(String username, String database, String query)
    {
        try
        {
            FileWriter queryFile = new FileWriter(VIRTUAL_MACHINE + "/Logs/Query_Log.txt", true);
            queryFile.append(getTime() + " | " + username + " | " + database + " | " + query + "\n");
            queryFile.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            crashReport(e);

        }
    }

    public void eventLog(String username, String database, String tableName, String query)
    {
        try
        {
            FileWriter fileWriter = new FileWriter(VIRTUAL_MACHINE + "/Logs/Event_Log.txt", true);
            fileWriter.append(getTime() +" | "+ username +" | "+ database + " | " + tableName + " | "  + query + "\n");
            fileWriter.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            crashReport(e);
        }
    }

    public void crashReport(Exception exp)
    {
        try
        {
            FileWriter fileWriter = new FileWriter(VIRTUAL_MACHINE + "/Logs/Event_Log.txt", true);
            fileWriter.append("Crash Occurred: " + exp + "\n");
            fileWriter.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            crashReport(e);
        }
    }

    public void transactionLog(String msg)
    {
        try
        {
            FileWriter fileWriter = new FileWriter(VIRTUAL_MACHINE + "/Logs/Event_Log.txt", true);
            fileWriter.append(msg + "\n");
            fileWriter.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            crashReport(e);
        }
    }

    public void transactionTime(float time)
    {
        try
        {
            FileWriter fileWriter = new FileWriter(VIRTUAL_MACHINE + "/Logs/Event_Log.txt", true);
            fileWriter.append("Total time taken for transaction execution:" + time + "\n");
            fileWriter.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            crashReport(e);
        }
    }

    public void generalLog(String username,String database, Long execTime)
    {
        try
        {
            FileWriter fileWriter = new FileWriter(VIRTUAL_MACHINE + "/Logs/General_Log.txt", true);
            String dirPath= VIRTUAL_MACHINE + "/" + database + "/";
            File fileIterate= new File(dirPath);
            int count=0;
            String str[] = fileIterate.list();
            int noOFLines = 0;
            for (int i = 0; i < str.length; i++) {
                String s = str[i];
                if(s.contains("metadata")) continue;
                File check = new File(fileIterate, s);
                if (check.isFile()) {
                    count++;
                    File table = new File(dirPath + check.getName());
                    BufferedReader br = new BufferedReader(new FileReader(table));
                    while (br.readLine() != null)
                        noOFLines++;
                }
            }
            fileWriter.append("Execution time: " + execTime + " | " + " User: "+ username + " | " + " Database: " + database + " has " + count + " tables with " + (noOFLines - count) + " records " + "\n");
            fileWriter.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            crashReport(e);
        }
    }

    public static void main(String args[])
    {
        System.out.println("Log files have been generated. You can view them at this location: " +
                System.getProperty("user.dir") + "\\" + VIRTUAL_MACHINE + "\\Logs\\");
    }
}
