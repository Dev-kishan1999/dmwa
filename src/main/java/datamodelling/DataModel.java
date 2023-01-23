package datamodelling;
import logmanagement.LogManagement;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import static DDBMS.D2_DB.BASE_DIRECTORY;
import static DDBMS.D2_DB.VIRTUAL_MACHINE;
import static queryimplementation.QueryImplementation.*;

public class DataModel
{
    LogManagement logger = new LogManagement();
    static List<String> cardinality = new ArrayList<>();

    public void dataModel(String database)
    {
        try
        {
            File VM = new File(BASE_DIRECTORY);
            List<String> checkDB = new ArrayList<>();
            for (String name : VM.list()) {
                checkDB.add(name);
            }
            String em = getFromMetadata(database);
            if(em.contains(database))
            {
                if(checkDB.contains(database))
                {
                    String s = getMetadata(database);
                    String[] names = s.split("\n");
                    FileWriter fileWriter = fileWrite(database);
                    fileWriter.write("\nInstance: " + em.split("\\|")[1].trim());
                    for (String si: names) {
                        String[] nameOfTable = si.split("\\|");
                        List<String> column_names = new ArrayList<>();
                        List<String> column_types = new ArrayList<>();
                        List<String> column_keys = new ArrayList<>();
                        List<String> Relation = new ArrayList<>();
                        columnGenerate(nameOfTable, column_names, column_types, column_keys, Relation);
                        tableFormat(fileWriter, nameOfTable, column_names, column_types, column_keys, Relation);
                    }
                    fileWriter.write("\n\nCardinality: ");
                    for(String s1 : cardinality)
                    {
                        fileWriter.write(s1 + "\n");
                    }
                    fileWriter.close();
                }
                else
                    System.out.println("Database Not Found!");
            }
            else
                System.out.println("Database Not Found!");
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }

    private String getFromMetadata(String database)
    {
        String em = "";
        try
        {
            BufferedReader br = new BufferedReader(new FileReader(VIRTUAL_MACHINE + "/Global_Data_Dictionary.txt"));
            String line = "";
            do
            {
                if(line.contains(database))
                {
                    em += line + "\n";
                }
            } while ((line = br.readLine()) != null);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
        return em;
    }

    private void columnGenerate(String[] nameOfTable, List<String> column_names, List<String> column_types, List<String> column_keys, List<String> Relation)
    {
        try
        {
            String[] group = nameOfTable[1].split("\\,");
            for (String s2: group) {
                String[] split = s2.split("\\s+");
                column_names.add(split[0]);
                column_types.add(split[1]);
                if(split.length>2) {
                    column_keys.add(split[2]);
                    //System.out.println(split[2]);
                }
                if(split.length>3){
                    String s = "";
                    String str = split[4];
                    String answer = str.substring(str.indexOf("(")+1, str.indexOf(")"));
                    s += nameOfTable[0] + "|" + split[0] + "   ->   " + split[4].replaceAll("\\(.*\\)", "" + "|" + answer);
                    Relation.add(s);
                    String ca = "";
                    ca = nameOfTable[0] + ":" + split[0] + "(n)  ->  " + split[4].replaceAll("\\(.*\\)", "" + ":" + str.substring(str.indexOf("(")+1, str.indexOf(")"))) + "(1)";
                    cardinality.add(ca);
                }
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }

    private void tableFormat(FileWriter fileWriter, String[] nameOfTable, List<String> column_names, List<String> column_types, List<String> column_keys, List<String> cardinality)
    {
        try
        {
            fileWriter.write("\n\nTable: " + nameOfTable[0]);
            fileWriter.write("\n------------------------------------------------");
            fileWriter.write(String.format("\n|%14s| %14s| %14s|","Column Name","Data Type", "Key Name"));
            fileWriter.write("\n------------------------------------------------");
            int count = 0;
            for(int d = 0; d< column_names.size(); d++) {
                if (column_keys.size()>count) {
                    //System.out.println("for" + column_keys.get(d));
                    fileWriter.write(String.format("\n|%14s| %14s| %14s|", column_names.get(d), column_types.get(d), column_keys.get(d)));
                }else {
                    fileWriter.write(String.format("\n|%14s| %14s| %14s|", column_names.get(d), column_types.get(d), ""));

                }
                count++;
            }

            fileWriter.write("\n------------------------------------------------\n");
            fileWriter.write("Relationship with: ");
            for (String s: cardinality) {
                fileWriter.write(s);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }

    private FileWriter fileWrite(String database) throws IOException
    {
        FileWriter fileWriter = new FileWriter(VIRTUAL_MACHINE + "/DataModelling/" + database + "_ERD.txt", false);
        fileWriter.write("Database: " + database);
        return fileWriter;
    }

    private String getMetadata(String database)
    {
        String s = "";
        try
        {
            File metaData = new File( VIRTUAL_MACHINE + "/" + database + "/" + database + "_metadata.txt");
            BufferedReader br = new BufferedReader(new FileReader(metaData));
            String line = "";
            if ((line = br.readLine()) != null) {
                do
                {
                    s += line + "\n";
                } while ((line = br.readLine()) != null);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
        return s;
    }

    public static void main(String args[])
    {
        String userName = args[0];
        System.out.println("Welcome to Data Modelling module!");
        Scanner input=new Scanner(System.in);
        System.out.println("Enter the database name: ");
        String s = input.nextLine();
        DataModel data = new DataModel();
        data.dataModel(s);
        System.out.println("Data Model for the database " + s + " has been generated.");
        System.out.println("You can check it at : " + VIRTUAL_MACHINE + "/DataModelling/" + s + "_ERD.txt");
    }
}
