package queryimplementation;
import de.vandermeer.asciitable.AsciiTable;
import logmanagement.LogManagement;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import static DDBMS.D2_DB.BASE_DIRECTORY;
import static DDBMS.D2_DB.VIRTUAL_MACHINE;

import static queryimplementation.QueryImplementation.*;

public class ExecuteQuery
{
    LogManagement logger = new LogManagement();
    ParseQuery pq = new ParseQuery();

    public void executeCreateDatabase(String username, String query)
    {
        try
        {
            String[] query_parts = query.split("\\s+");
            String database = query_parts[2];

            new File(BASE_DIRECTORY + database).mkdirs();

            String meta_data_file_name = BASE_DIRECTORY + database + "/" + database + "_metadata.txt";
            new File(meta_data_file_name).createNewFile();

            FileWriter fw_global = new FileWriter(BASE_DIRECTORY + GLOBAL_METADATA_FILE, true);
            FileWriter fw_local = new FileWriter(BASE_DIRECTORY + LOCAL_METADATA_FILE, true);


            fw_global.write(database + "|" + VIRTUAL_MACHINE + "\n");

            fw_local.write(database + "|\n");

            fw_global.close();
            fw_local.close();

            if (!isTransaction) {
                System.out.println("Database " + database + " has been created!");
            }
            logger.eventLog(username, database, TABLE_NAME, query);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }

    public void executeCreateTable(String username, String query)
    {
        try
        {
            int firstParenthesis=0, lastParenthesis=0;
            for (int index=0; index<query.length(); index++) {
                if (Character.compare(query.toCharArray()[index], '(') == 0) {
                    firstParenthesis = index;
                    break;
                }
            }
            for (int index=query.length()-1; index>=0; index--) {
                if (Character.compare(query.toCharArray()[index], ')') == 0) {
                    lastParenthesis = index;
                    break;
                }
            }

            TABLE_NAME = query.substring(0, firstParenthesis).trim().split("\\s+")[2].trim();


            // Getting column names and data types
            List<String> column_names = new ArrayList<>();
            List<String> column_types = new ArrayList<>();
            String primary_key=null;
            String foreign_key=null, fk_details=null;


            String[] column_parts = query.substring(firstParenthesis+1,lastParenthesis).trim().split(",");
            for (String s : column_parts) {
                if (!(s.contains("primary key") || s.contains("foreign key"))) {

                    String[] s_parts = s.trim().split("\\s+");
                    column_names.add(s_parts[0]);
                    column_types.add(s_parts[1]);
                } else if (s.contains("primary key")) {
                    primary_key = s.substring(s.indexOf("(") + "(".length());
                    primary_key = primary_key.substring(0, primary_key.indexOf(")")).trim();
                } else if (s.contains("foreign key")) {
                    fk_details = "FOREIGN_KEY ";

                    foreign_key = s.substring(s.indexOf("(") + "(".length());
                    foreign_key = foreign_key.substring(0, foreign_key.indexOf(")")).trim();

                    fk_details += "REFERENCES" + s.substring(s.indexOf("references")+"references".length());
                    String temp = fk_details;
                    temp = temp.substring(temp.indexOf("REFERENCES ") + "REFERENCES ".length());
                    temp = temp.replaceAll("\\s+", "");

                    fk_details = fk_details.substring(0, fk_details.indexOf("REFERENCES ") + "REFERENCES ".length()).trim();
                    fk_details += " " + temp;
                }
            }

            String metadata_header = "";
            String header = "";
            for (int i=0; i<column_names.size(); i++) {
                header += column_names.get(i) + "|";

                if (column_names.get(i).equals(primary_key)) {
                    metadata_header += column_names.get(i) + " " + column_types.get(i) + " PRIMARY_KEY,";
                } else if (column_names.get(i).equals(foreign_key)) {
                    metadata_header += column_names.get(i) + " " + column_types.get(i) + " " + fk_details + ",";
                } else {
                    metadata_header += column_names.get(i) + " " + column_types.get(i) + ",";
                }
            }
            metadata_header = metadata_header.substring(0, metadata_header.length()-1);
            header = header.substring(0, header.length()-1) + "\n";

            // add to local metadata files
            String meta_data_file_name = BASE_DIRECTORY + DATABASE + "/" + DATABASE + "_metadata.txt";
            FileWriter fw_local = new FileWriter(meta_data_file_name, true);
            fw_local.write(TABLE_NAME + "|" + metadata_header + "\n");
            fw_local.close();


            // add table name to Local_Meta_Data.txt file
            List<String> data = new ArrayList<String>();
            String file_path = BASE_DIRECTORY + LOCAL_METADATA_FILE;

            File metadata = new File(file_path);
            Scanner reader = new Scanner(metadata);
            while(reader.hasNextLine()) {
                String line = reader.nextLine();
                String[] line_parts = line.split("\\|");

                if (line_parts[0].equals(DATABASE)) {
                    if (line_parts.length > 1) {
                        line = line.substring(0, line.length());
                        line += "," + TABLE_NAME + "\n";
                    } else {
                        line = DATABASE + "|" + TABLE_NAME + "\n";
                    }
                } else {
                    line += "\n";
                }
                data.add(line);
            }

            FileWriter md = new FileWriter(file_path, false);
            for (String d : data) {
                md.write(d);
            }
            md.close();


            // create table file
            String table_filepath = BASE_DIRECTORY + DATABASE + "/" + TABLE_NAME + ".txt";
            File table = new File(table_filepath);
            table.createNewFile();
            FileWriter t = new FileWriter(table_filepath);
            t.write(header);
            t.close();

            if (!isTransaction) {
                System.out.println("Table " + TABLE_NAME + " has been created!");
            }

            logger.eventLog(username, DATABASE, TABLE_NAME, query);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }

    public void executeCreate(String username, String query)
    {
        try
        {
            String[] query_parts = query.split("\\s+");
            String createWhat = query_parts[1];

            if (createWhat.equals("database"))
            {
                executeCreateDatabase(username, query);
            }
            else if (createWhat.equals("table"))
            {
                executeCreateTable(username, query);
            }
            else
            {
                System.out.println("Invalid!");
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }

    public void executeUse(String username, String query) {
        String[] queryParts = query.split("\\s+");
        String database = queryParts[1];

        DATABASE = database;
        System.out.println("Database changed!");
    }

    public void executeInsert(String username, String query)
    {
        try
        {
            // get table_name
            String[] query_parts = query.split("\\s+");
            TABLE_NAME = query_parts[2];

            String file_path = BASE_DIRECTORY + DATABASE + "/" + TABLE_NAME + ".txt";

            String value_str = query.substring(query.indexOf("values") + "values".length()).trim();
            value_str = value_str.substring(1, value_str.length()-1).trim();

            String[] values = value_str.split(",");
            String data = "";

            for (String value : values) {
                value = value.trim();
                if (value.contains("'")) {
                    value = value.substring(1,value.length()-1);
                }

                data += value + "|";
            }
            data = data.substring(0, data.length()-1).trim() + "\n";

            FileWriter fw = new FileWriter(file_path, true);
            fw.write(data);
            fw.close();

            System.out.println("Values have been inserted into " + TABLE_NAME + " table!");
            logger.eventLog(username, DATABASE, TABLE_NAME, query);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }

    public void executeSelect(String username, String query)
    {
        try
        {
            String[] query_parts = query.trim().split("\\s+");

            List<String> data = new ArrayList<String>();
            List<String> column_names = new ArrayList<String>();
            String q = query;

            // get table name
            if (query.contains("where")) {
                q = q.substring(0, q.indexOf("where")-1);
                int size = q.split("\\s+").length;
                TABLE_NAME = q.split("\\s+")[q.split("\\s+").length-1];
            } else {
                int size = q.split("\\s+").length;
                TABLE_NAME = q.split("\\s+")[q.split("\\s+").length-1];
            }

            String file_path = BASE_DIRECTORY + DATABASE + "/" + TABLE_NAME + ".txt";

            // get column names
            String header=null;
            File f = new File(file_path);
            Scanner sc = new Scanner(f);
            while (sc.hasNextLine()) {
                header = sc.nextLine();
                break;
            }
            String[] column_n = header.split("\\|");
            for (String c : column_n) {
                column_names.add(c);
            }

            // Check for * or column names
            if (query_parts[1].equals("*")) {
                if (query.contains("where")) {
                    String column_name = query.substring(query.indexOf("where ") + "where ".length());
                    column_name = column_name.substring(0, column_name.indexOf("=")).trim();

                    String value = query.substring(query.indexOf("=") + "=".length()).trim();
                    if (value.contains("'")) {
                        value = value.substring(1, value.length()-1);
                    }

                    int column_number=0;
                    for (int j=0; j<column_names.size(); j++) {
                        if (column_names.get(j).equals(column_name)) {
                            column_number = j;
                            break;
                        }
                    }

                    data.add(header);
                    if (value.contains("'")) {
                        value = value.trim();
                        value = value.substring(1, value.length()-1).trim();
                    }
                    File file = new File(file_path);
                    Scanner scanner = new Scanner(file);
                    while (scanner.hasNextLine()) {
                        String d = scanner.nextLine();
                        String[] d_parts = d.split("\\|");
                        if (d_parts[column_number].equals(value)) {
                            data.add(d);
                        }
                    }
                    scanner.close();

                } else {
                    // where is not included
                    // All data
                    File file = new File(file_path);
                    Scanner reader = new Scanner(file);
                    while (reader.hasNextLine()) {
                        String d = reader.nextLine();
                        data.add(d);
                    }
                    reader.close();
                }
            } else {
                // Column names

                if (query.contains("where")) {
                    String column_name = query.substring(query.indexOf("where ") + "where ".length()).trim();
                    column_name = column_name.substring(0, column_name.indexOf("=")).trim();

                    String value = query.substring(query.indexOf("=") + "=".length()).trim();
                    if (value.contains("'")) {
                        value = value.substring(1, value.length()-1);
                    }

                    // get require column names and number
                    List<Integer> column_numbers = new ArrayList<Integer>();
                    String c_names = query.substring(query.indexOf("select ") + "select".length(), query.indexOf("from"));


                    String[] c_names_part = c_names.split(",");
                    header = c_names.trim().replaceAll("\\s+", "").replaceAll(",", "|");
                    data.add(header);


                    // get column numbers
                    for (String cn : c_names_part) {
                        for (int k=0; k<column_names.size(); k++) {
                            if (cn.trim().equals(column_names.get(k).trim())) {
                                column_numbers.add(k);
                            }
                        }
                    }

                    int column_number=0;
                    for (int j=0; j<column_names.size(); j++) {
                        if (column_names.get(j).equals(column_name)) {
                            column_number = j;
                            break;
                        }
                    }

                    File file = new File(file_path);
                    Scanner scanner = new Scanner(file);
                    while (scanner.hasNextLine()) {
                        String d = scanner.nextLine();
                        String[] d_parts = d.split("\\|");
                        if (d_parts[column_number].equals(value)) {
                            String v = "";
                            for (int number : column_numbers) {
                                v += d_parts[number] + "|";
                            }
                            v = v.substring(0, v.length()-1);
                            data.add(v);
                        }
                    }
                    scanner.close();

                } else {
                    // where is not included

                    // get require column names and number
                    List<Integer> column_numbers = new ArrayList<Integer>();
                    String c_names = query.substring(query.indexOf("select ") + "select".length(), query.indexOf("from"));

                    String[] c_names_part = c_names.split(",");
                    header = c_names.trim().replaceAll("\\s+", "").replaceAll(",", "|");
//                data.add(header);

                    // get column numbers
                    for (String cn : c_names_part) {
                        for (int k=0; k<column_names.size(); k++) {
                            if (cn.trim().equals(column_names.get(k).trim())) {
                                column_numbers.add(k);
                            }
                        }
                    }

                    File file = new File(file_path);
                    Scanner scanner = new Scanner(file);
                    while (scanner.hasNextLine()) {
                        String d = scanner.nextLine();
                        String[] d_parts = d.split("\\|");

                        String v = "";
                        for (int number : column_numbers) {
                            v += d_parts[number] + "|";
                        }
                        v = v.substring(0, v.length()-1);
                        data.add(v);
                    }
                    scanner.close();
                }

            }
            AsciiTable at = new AsciiTable();
            boolean is_header = true;

            for (String d : data) {
                if(is_header) {
                    at.addRule();
                    at.addRow(d.split("\\|")).setPaddingLeft(2);
                    at.addRule();
                    is_header = false;
                } else {
                    if (data.size() > 1) {
                        at.addRow(d.split("\\|")).setPaddingLeft(2);
                    }
                }
            }
            at.addRule();
            String render_table = at.render();
            System.out.println(render_table);

            logger.eventLog(username, DATABASE, TABLE_NAME, query);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }

    public void executeUpdate(String username, String query)
    {
        try
        {
            String[] querySplit = query.split("\\s+");
            TABLE_NAME = querySplit[1].trim();
            String tablePath = BASE_DIRECTORY + DATABASE + "/" + TABLE_NAME + ".txt";

            int indexOfSet = query.indexOf("set");
            int indexOfWhere = query.indexOf("where");

            String columnAndValue = query.substring(indexOfSet + 4, indexOfWhere - 1);
            columnAndValue = columnAndValue.trim();

            String condition = querySplit[querySplit.length - 1];
            condition = condition.substring(0, condition.length());

            String[] temp = columnAndValue.split("=");
            String updateColumn = temp[0].trim();
            String updateValue = temp[1].trim();
            if(updateValue.startsWith("'") || updateValue.startsWith("\""))
            {
                updateValue = updateValue.substring(1, updateValue.length() - 1);
            }

            String[] conditionBreak = condition.split("=");
            String conditionColumn = conditionBreak[0];
            String conditionValue = conditionBreak[1];
            if(conditionValue.startsWith("'") || conditionValue.startsWith("\""))
            {
                conditionValue = conditionValue.substring(1, conditionValue.length() - 1);
            }

            boolean answer = true;

            try
            {
                String line = "";
                File metadata = new File(BASE_DIRECTORY + DATABASE + "/" + DATABASE + "_metadata.txt");;
                BufferedReader checkForUpdateType = new BufferedReader(new FileReader(metadata));
                while((line = checkForUpdateType.readLine()) != null)
                {
                    String[] allLines = line.split("\n");
                    for(String everyLine : allLines)
                    {
                        String[] values = everyLine.split("\\|");
                        if(values[0].equals(TABLE_NAME))
                        {
                            String columnsAndTypes = values[1];
                            String[] types = columnsAndTypes.split(",");
                            for(int j = 0; j < types.length; j++)
                            {
                                if(types[j].startsWith(updateColumn))
                                {
                                    String[] isPkOrFk = types[j].split(" ");
                                    if(isPkOrFk.length == 3 || isPkOrFk.length == 5)
                                    {
                                        if(isPkOrFk[2].equals("PRIMARY_KEY"))
                                        {
                                            System.out.println("You are trying to update a primary key which is not possible.");
                                            System.out.println("The query cannot be executed.");
                                            answer = false;
                                        }
                                        else if(isPkOrFk[2].equals("FOREIGN_KEY"))
                                        {
                                            System.out.println("You are trying to update a foreign key which is not possible.");
                                            System.out.println("The query cannot be executed");
                                            answer = false;
                                        }
                                    }

                                    else if(isPkOrFk.length == 2)
                                    {
                                        answer = true;
                                    }
                                }
                            }
                        }
                    }
                }

                checkForUpdateType.close();

                if(answer)
                {
                    String overWrite = "";
                    String tableRow = "";
                    int indexOfConditionColumn = -1;
                    int indexOfUpdateColumn = -1;
                    File table = new File(tablePath);
                    BufferedReader updating = new BufferedReader(new FileReader(table));
                    int count = 0;
                    String tableHeader = updating.readLine();
                    overWrite += tableHeader;
                    overWrite += "\n";
                    String[] headerValues = tableHeader.split("\\|");
                    for(int x = 0; x < headerValues.length; x++)
                    {
                        if(headerValues[x].equals(conditionColumn))
                        {
                            indexOfConditionColumn = x;
                        }

                        if(headerValues[x].equals(updateColumn))
                        {
                            indexOfUpdateColumn = x;
                        }
                    }

                    while((tableRow = updating.readLine()) != null)
                    {
                        String[] rowValues = tableRow.split("\\|");
                        if(rowValues[indexOfConditionColumn].equals(conditionValue))
                        {
                            rowValues[indexOfUpdateColumn] = updateValue;
                        }

                        for(int x = 0; x < rowValues.length; x++)
                        {
                            if(x == rowValues.length - 1)
                            {
                                overWrite += rowValues[x];
                            }
                            else
                            {
                                overWrite += rowValues[x];
                                overWrite += "|";
                            }
                        }
                        overWrite += "\n";
                    }

                    updating.close();

                    FileWriter writer = new FileWriter(tablePath, false);
                    writer.write(overWrite);
                    writer.close();

                    System.out.println("Values in the table " + TABLE_NAME + " have been updated!");
                    logger.eventLog(username, DATABASE, TABLE_NAME, query);
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
                logger.crashReport(e);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }

    public void executeDelete(String username, String query)
    {
        try
        {
            String[] querySplit = query.split("\\s+");
            String TABLE_NAME = querySplit[2].trim();
            String tablePath = BASE_DIRECTORY + DATABASE + "/" + TABLE_NAME + ".txt";

            int indexOfWhere = query.indexOf("where");
            String condition = querySplit[querySplit.length - 1];
            condition = condition.substring(0, condition.length());
            String[] conditionBreak = condition.split("=");
            String conditionColumn = conditionBreak[0];
            String conditionValue = conditionBreak[1];
            if(conditionValue.startsWith("'") || conditionValue.startsWith("\""))
            {
                conditionValue = conditionValue.substring(1, conditionValue.length() - 1);
            }

            boolean answer = true;

            try
            {
                String line = "";
                File metadata = new File(BASE_DIRECTORY + DATABASE + "/" + DATABASE + "_metadata.txt");
//                String wholeFile = Files.readString(Path.of(BASE_DIRECTORY + DATABASE + "/" + DATABASE + "_metadata.txt"));
                String wholeFile = "";
                File f = new File (BASE_DIRECTORY + DATABASE + "/" + DATABASE + "_metadata.txt");
                Scanner sc = new Scanner(f);
                while (sc.hasNextLine()) {
                    String data = sc.nextLine();
                    wholeFile += data;

                }
                BufferedReader checkForDeleteType = new BufferedReader(new FileReader(metadata));
                while((line = checkForDeleteType.readLine()) != null)
                {
                    String[] allLines = line.split("\n");
                    for(String everyLine : allLines)
                    {
                        String[] values = everyLine.split("\\|");
                        if(values[0].equals(TABLE_NAME))
                        {
                            String pkColumn = "";
                            String columnsAndTypes = values[1];
                            String[] types = columnsAndTypes.split(",");
                            for(int j = 0; j < types.length; j++)
                            {
                                String[] pkwords = types[j].split(" ");
                                if(pkwords.length == 3)
                                {
                                    pkColumn = pkwords[0];
                                }

                                if(types[j].startsWith(conditionColumn))
                                {
                                    String pattern = "REFERENCES " + TABLE_NAME + "(" + pkColumn + ")";
                                    if(wholeFile.contains(pattern))
                                    {
                                        System.out.println("You are trying to delete a row which has foreign key contraint. " +
                                                "Please delete the foreign key first before deleting primary key");
                                        answer = false;
                                    }
                                    else
                                    {
                                        answer = true;
                                    }
                                }
                            }
                        }
                    }
                }

                checkForDeleteType.close();

                if(answer)
                {
                    String overWrite = "";
                    String tableRow = "";
                    int indexOfConditionColumn = -1;
                    File table = new File(tablePath);
                    BufferedReader deleting = new BufferedReader(new FileReader(table));
                    int count = 0;
                    String tableHeader = deleting.readLine();
                    overWrite += tableHeader;
                    overWrite += "\n";
                    String[] headerValues = tableHeader.split("\\|");
                    for(int x = 0; x < headerValues.length; x++)
                    {
                        if(headerValues[x].equals(conditionColumn))
                        {
                            indexOfConditionColumn = x;
                        }
                    }

                    while((tableRow = deleting.readLine()) != null)
                    {
                        String[] rowValues = tableRow.split("\\|");
                        if(!rowValues[indexOfConditionColumn].equals(conditionValue))
                        {
                            for(int x = 0; x < rowValues.length; x++)
                            {
                                if(x == rowValues.length - 1)
                                {
                                    overWrite += rowValues[x];
                                }
                                else
                                {
                                    overWrite += rowValues[x];
                                    overWrite += "|";
                                }
                            }
                            overWrite += "\n";
                        }
                    }

                    deleting.close();

                    FileWriter writer = new FileWriter(tablePath, false);
                    writer.write(overWrite);
                    writer.close();

                    System.out.println("Values from the table " + TABLE_NAME + " have been deleted!");
                    logger.eventLog(username, DATABASE, TABLE_NAME, query);
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
                logger.crashReport(e);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }

    public void executeQuery(String username, String query)
    {
        query = pq.formatQuery(query.toLowerCase().trim());

        String queryType = query.split("\\s+")[0];

        switch (queryType)
        {
            case "create":
                executeCreate(username, query);
                break;

            case "use":
                executeUse(username, query);
                break;

            case "insert":
                executeInsert(username, query);
                break;

            case "select":
                executeSelect(username, query);
                break;

            case "update":
                executeUpdate(username, query);
                break;

            case "delete":
                executeDelete(username, query);
                break;

            default:
                System.out.println("INVALID QUERY! - " + queryType.toUpperCase());
        }
    }
}
