package datadump;
import logmanagement.LogManagement;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import static DDBMS.D2_DB.BASE_DIRECTORY;

public class DataDumpHandler
{
    List<String> createQueries=new ArrayList<>();
    List<String> dataDump=new ArrayList<>();
    public static String DUMP_FOLDER = "SQLDump/";
    LogManagement logger = new LogManagement();

    public enum GenerateType
    {
        DATABASE,
        TABLE
    }

    public DataDumpHandler() {}

    public void writeGenerateQuery( String path, String pathFile, List<String> queries, List<String> data)
    {
        try
        {
            File theDir = new File(path);
            if (!theDir.exists())
            {
                boolean result = false;
                try
                {
                    FileWriter fw_local = new FileWriter(pathFile, true);

                    for(int i = 0; i < queries.size(); i++)
                    {
                        if(queries.size() > 0)
                        {
                            fw_local.write(queries.get(i));
                        }
//                        fw_local.write(queries.get(i));
//                        fw_local.write(data.get(i));
                    }

                    for(int i = 0; i < data.size(); i++)
                    {

                        if(data.size() > 0)
                        {
                            fw_local.write(data.get(i));
                        }

                    }

                    fw_local.close();
                    result = true;
                }
                catch (SecurityException se)
                {
                    System.out.println(se.getMessage());
                    logger.crashReport(se);
                }
            }
            else if (theDir.exists())
            {
                FileWriter fw_local = new FileWriter(pathFile, true);

                for (String query:queries)
                {
                    fw_local.write(query);
                }
                fw_local.close();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }

    public void exportDump(String databaseName)
    {
        try
        {
            String filePath = BASE_DIRECTORY+databaseName+"/"+databaseName+"_metadata.txt";
            generateCreateQuery(filePath);
            generateInsertQuery(filePath,databaseName);
            writeGenerateQuery(BASE_DIRECTORY+ DUMP_FOLDER +databaseName,BASE_DIRECTORY+ DUMP_FOLDER +databaseName+"_SqlDump.sql",createQueries, dataDump);
            System.out.println("SQL Dump file for this database path is : " + System.getProperty("user.dir") + BASE_DIRECTORY+ DUMP_FOLDER + databaseName +"_SqlDump.sql");
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }

    public void generateInsertQuery(String filePath, String databaseName)
    {
        try
        {
            File metadata = new File(filePath);
            Scanner reader = new Scanner(metadata);
            while(reader.hasNextLine())
            {
                String line = reader.nextLine();
                String[] line_parts = line.split("\\|");
                String tableName=line_parts[0];
                String tablePath=BASE_DIRECTORY+databaseName+"/"+tableName+".txt";

                File table = new File(tablePath);
                Scanner tableReader = new Scanner(table);
                boolean header=true;
                StringBuilder dataBuilder=new StringBuilder();
                dataBuilder.append("INSERT INTO ");
                dataBuilder.append(tableName);
                dataBuilder.append(" VALUES ");
                boolean hasValue = false;
                while(tableReader.hasNextLine())
                {

                    String currLine = tableReader.nextLine();
                    if(header)
                    {
                        header = false;
                        continue;
                    }
                    hasValue = true;
                    String []dataArr=currLine.split("\\|");
                    dataBuilder.append("(");
                    for(int i=0; i<dataArr.length;i++){
                        dataBuilder.append(dataArr[i]);
                        dataBuilder.append(",");
                    }
                    dataBuilder.deleteCharAt(dataBuilder.length()-1);
                    dataBuilder.append("), ");
                }
                dataBuilder = new StringBuilder(dataBuilder.substring(0, dataBuilder.length() - 2));
                dataBuilder.append(";");
                dataBuilder.append("\n");
                dataBuilder.append("\n");
                if(hasValue)
                {
                    dataDump.add(dataBuilder.toString());
                }
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }

    public void generateCreateQuery(String filePath)
    {
        try
        {
            StringBuilder queryBuilder;

            File metadata = new File(filePath);
            Scanner reader = new Scanner(metadata);
            while(reader.hasNextLine())
            {
                queryBuilder=new StringBuilder();
                String line = reader.nextLine();
                String[] line_parts = line.split("\\|");
                String tableName=line_parts[0];
                queryBuilder.append("DROP TABLE IF EXISTS '");
                queryBuilder.append(tableName);
                queryBuilder.append("';");
                queryBuilder.append("\n");
                queryBuilder.append("CREATE TABLE ");
                queryBuilder.append(tableName);
                queryBuilder.append(" (");
                String columnInfo=line_parts[1];

                String[] column_parts = columnInfo.split(",");
                String []primaryKeyInfo=new String[3];
                String []foreignKeyInfo=new String[5];
                for (String parts:column_parts)
                {
                    String []colInfo = parts.split("\\s");
                    int length = colInfo.length;
                    if(length==3){
                        primaryKeyInfo=colInfo;
                    }else if(length==5){
                        foreignKeyInfo=colInfo;
                    }
                    for(int i=0;i<2;i++ ){
                        queryBuilder.append(colInfo[i]);
                        if(i==1)
                        {
                            queryBuilder.append(",");
                        }
                        queryBuilder.append(" ");

                    }

                }
                if(primaryKeyInfo[0]==null && foreignKeyInfo[0]==null)
                {
                    queryBuilder.delete(queryBuilder.length()-2,queryBuilder.length());
                    queryBuilder.append(");");
                    queryBuilder.append("\n");
                    createQueries.add(queryBuilder.toString());
                    continue;
                }
                else if(foreignKeyInfo[0]==null && primaryKeyInfo[0] != null)
                {
                    queryBuilder.append("PRIMARY KEY (");
                    queryBuilder.append(primaryKeyInfo[0]);
                    queryBuilder.append("));");
                    queryBuilder.append("\n");
                    createQueries.add(queryBuilder.toString());
                    continue;
                }
                else if(primaryKeyInfo[0]==null)
                {
                    queryBuilder.append("FOREIGN KEY (");
                    queryBuilder.append(foreignKeyInfo[0]);
                    queryBuilder.append(") REFERENCES ");
                    queryBuilder.append(foreignKeyInfo[4]);
                    queryBuilder.append(");");
                    queryBuilder.append("\n");
                    createQueries.add(queryBuilder.toString());
                    continue;
                }

                queryBuilder.append("PRIMARY KEY (");
                queryBuilder.append(primaryKeyInfo[0]);
                queryBuilder.append("), ");
                queryBuilder.append("FOREIGN KEY (");
                queryBuilder.append(foreignKeyInfo[0]);
                queryBuilder.append(") REFERENCES ");
                queryBuilder.append(foreignKeyInfo[4]);
                queryBuilder.append(")");
                queryBuilder.append(";");
                queryBuilder.append("\n");
                createQueries.add(queryBuilder.toString());
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }
}
