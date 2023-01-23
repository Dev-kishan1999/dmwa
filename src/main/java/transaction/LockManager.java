package transaction;
import logmanagement.LogManagement;
import java.io.*;
import java.util.Arrays;
import java.util.Locale;

import static DDBMS.D2_DB.VIRTUAL_MACHINE;

public class LockManager
{
    LogManagement logger = new LogManagement();

    public boolean acquireLock(String query,String transactionId) throws Exception{
        String tableName = getTableName(query);
        int checker=reader(tableName,transactionId);

        // 0 = lock is already taken
        // 1 = denied lock acquisition
        // 4 = can acquire lock
        switch (checker)
        {
            case 0:
               // System.out.println("lock already taken on table: "+tableName+" by transaction: "+transactionId);
                return true;

            case 1:
               // System.out.println("Cannot provide lock on table: "+tableName+" because taken by: "+transactionId);
                return false;

            case 4:
            case 5:
                writer(tableName,transactionId);
               // System.out.println("Lock given to transaction : "+transactionId+" on table: "+tableName);
                return true;

            default:
                return false;

        }
    }

    public String getTableName(String query)
    {
        String tableName;
        String[] words = query.toLowerCase(Locale.ROOT).split("\\s+");
        if(words[0].equalsIgnoreCase("select")){
            int indexOfFrom = Arrays.asList(words).indexOf("from");
            return words[indexOfFrom+1].replaceAll("[^a-zA-Z0-9_]","");
        }
        else if(words[0].equalsIgnoreCase("create") && words[1].equalsIgnoreCase("table")) {

              String[] table = words[2].split("\\(");
              return table[0];
        } else if(words[0].equalsIgnoreCase("update")){
            return words[1];
        }
        else {
            tableName = words[2];
            return tableName.replaceAll("[^a-zA-Z0-9_]","");
        }
    }

    private void writer(String tableName,String transactionId)
    {
        try
        {
            File file = new File(VIRTUAL_MACHINE + "/transaction.txt");
            if(!file.exists()){
                file.createNewFile();
            }
            FileWriter fileWriter = new FileWriter(file,true);
            fileWriter.append(transactionId+";"+tableName+"\n");
            fileWriter.flush();
            fileWriter.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }

    private int reader(String tableName,String transactionId)
    {
        int answer = -1;
        try
        {
            File file = new File(VIRTUAL_MACHINE + "/transaction.txt");
            if(!file.exists()){
                file.createNewFile();
                return 5;
            }

            BufferedReader bf = new BufferedReader(new FileReader(file));
            String line =bf.readLine();

            if(line == null){
                bf.close();
                return 5;
            }

            do{
                int checker = lockChecker(tableName,transactionId,line);
                if( checker == 0) {
                  //  System.out.println("lock is already taken.");
                    return 0;
                } else if(checker == 1) {
                   // System.out.println("table acquired by another table.");
                    return 1;
                }
            }
            while((line=bf.readLine()) != null);
            answer = 4;
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
        return answer;
    }

    private int lockChecker(String tableName, String transactionId,String line)
    {

        String[] transactionWords = line.split(";");
        int flag = 0;
        if(transactionWords[1].equalsIgnoreCase(tableName)){
            if(transactionWords[0].equalsIgnoreCase(transactionId)){
                //same table same transaction
                return 0;
            } else{
                //same table different transaction
                return 1;
            }
        } else {
            if(transactionWords[0].equalsIgnoreCase(transactionId)){
                //different table same transaction
                return 2;
            } else {
                //different table different transaction
                return 3;
            }
        }
    }

    public void releaseLock()
    {
        try
        {
            File file = new File(VIRTUAL_MACHINE + "/transaction.txt");
            FileWriter fileWriter = new FileWriter(file);
            fileWriter.flush();
            fileWriter.close();
            file.delete();
           // System.out.println("Lock released!!");
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }
}
