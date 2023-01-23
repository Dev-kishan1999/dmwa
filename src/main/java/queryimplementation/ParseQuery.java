package queryimplementation;
import logmanagement.LogManagement;
import transaction.Transaction;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static DDBMS.D2_DB.BASE_DIRECTORY;
import static queryimplementation.ExecuteQuery.*;
import static queryimplementation.QueryImplementation.*;

public class ParseQuery 
{
    // VARCHAR LIMIT (1-500) REGEX ------------------> \(([1-9]|[1-9][0-9]|[1-4][0-9][0-9]|500)\)

    // create database Info;
    public static String REGEX_FOR_QUERY_CREATE_DATABASE = "\\s*create\\s+database\\s+[0-9a-zA-Z_]+\\s*;\\s*";

    // create table Orders(OrderID int,OrderNumber int,PersonID int,primary key (OrderID),foreign key (PersonID) references Persons(PersonID));
    public static String REGEX_FOR_QUERY_CREATE_TABLE = "\\s*create\\s+table\\s+[0-9a-zA-Z_]+\\s*\\(\\s*([0-9a-zA-Z_]+\\s+(int|float|boolean|varchar\\(([1-9]|[1-9][0-9]|[1-4][0-9][0-9]|500)\\))\\s*,\\s*)*[0-9a-zA-Z_]+\\s+(int|float|boolean|varchar\\(([1-9]|[1-9][0-9]|[1-4][0-9][0-9]|500)\\))(\\s*,\\s*primary\\s+key\\s*\\(\\s*[0-9a-zA-Z_]+\\s*\\))?(\\s*,\\s*foreign\\s+key\\s*\\(\\s*[0-9a-zA-Z_]+\\s*\\)\\s+references\\s+[0-9a-zA-Z_]+\\s*\\(\\s*[0-9a-zA-Z_]+\\s*\\))*\\s*\\)\\s*;\\s*";

    // use Info;
    public static String REGEX_FOR_QUERY_USE = "\\s*use\\s+[0-9a-zA-Z_]+\\s*;\\s*";

    // insert into Students values ('kalpit', 51);
    // insert into Students (name, id) values ('kalpit', 51);
    public static String REGEX_FOR_QUERY_INSERT = "\\s*insert\\s+into\\s+[0-9a-zA-Z_]+(\\s*\\(\\s*([0-9a-zA-Z_]+\\s*,\\s*)*[0-9a-zA-Z_]+\\s*\\))?\\s+values\\s*\\(\\s*(('[0-9a-zA-Z _?!@&*()-]*'|\\d+)\\s*,\\s*)*('[0-9a-zA-Z _?!@&*()-]*'|\\d+)\\s*\\)\\s*;\\s*";

    // select * from Students;
    // select * from Students where id=51;
    // select column1, column2 from students where id=5;
    public static String REGEX_FOR_QUERY_SELECT = "\\s*select\\s+(\\*|([0-9a-zA-Z_]+\\s*,\\s*)*[0-9a-zA-Z_]+)\\s+from\\s+[0-9a-zA-Z_]+\\s*(\\swhere\\s+[0-9a-zA-Z_]+\\s*=\\s*('[0-9a-zA-Z _?!@&*()-]*'|\\d+))?;\\s*";

    // update Students set column1='v1' where id='a2c';
    public static String REGEX_FOR_QUERY_UPDATE = "\\s*update\\s+[0-9a-zA-Z_]+\\s+set\\s+[0-9a-zA-Z_]+\\s*=\\s*('[0-9a-zA-Z _?!@&*()-]*'|\\d+)\\s+where\\s+[0-9a-zA-Z_]+\\s*=\\s*('[0-9a-zA-Z _?!@&*()-]*'|\\d+)\\s*;\\s*";

    // delete from Students where ID = 51;
    public static String REGEX_FOR_QUERY_DELETE = "\\s*delete\\s+from\\s+[0-9a-zA-Z_]+\\s+where\\s+[0-9a-zA-Z_]+\\s*=\\s*('[0-9a-zA-Z _?!@&*()-]*'|\\d+)\\s*;\\s*";

    // start transaction;
    public static String REGEX_FOR_START_TRANSACTION = "\\s*start\\s+transaction\\s*;\\s*";

    // commit;
    public static String REGEX_FOR_COMMIT_TRANSACTION = "\\s*commit\\s*;\\s*";

    // rollback;
    public static String REGEX_FOR_ROLLBACK_TRANSACTION = "\\s*rollback\\s*;\\s*";


    // ---------------------------------------------------------------------------------------------------------------


    public static List<String> DATABASES = new ArrayList<>();
    public static List<String> LOCAL_DATABASES = new ArrayList<>();
    public static List<String> GLOBAL_DATABASES = new ArrayList<>();
    LogManagement logger = new LogManagement();

    public void getDatabase()
    {
        try
        {
            File global_metadata = new File(BASE_DIRECTORY + GLOBAL_METADATA_FILE);
            Scanner reader = new Scanner(global_metadata);
            while(reader.hasNextLine()) 
            {
                String line = reader.nextLine();
                String[] line_parts = line.split("\\|");

                if (line_parts[1] == "global") {
                    GLOBAL_DATABASES.add(line_parts[0]);
                }

                if (line_parts[1] == "local") {
                    LOCAL_DATABASES.add(line_parts[0]);
                }

                DATABASES.add(line_parts[0]);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }

    public String formatQuery(String query) 
    {
        return query.substring(0, query.length()-1).trim().replaceAll("\\s+", " ");
    }

    public boolean matchQuery(Matcher matcher, String queryType) 
    {
        if (matcher.matches()) 
        {
            return true;
        } 
        else 
        {
            return false;
        }
    }

    public boolean parseCreateDatabase(String userName, String query)
    {
        boolean answer = false;
        try
        {
            Matcher matcher = Pattern.compile(REGEX_FOR_QUERY_CREATE_DATABASE).matcher(query);
            if (matchQuery(matcher, "CREATE DATABASE"))
            {
                query = formatQuery(query.trim());
                String[] query_parts = query.split("\\s+");
                String database = query_parts[2];
                getDatabase();

                for (String db : DATABASES)
                {
                    if (db.equals(database))
                    {
                        System.out.println("Database already exists!");
                        return false;
                    }
                }
                answer = true;
            }
            else
            {
                answer = false;
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
        return answer;
    }

    public boolean parseCreateTable(String userName, String query)
    {
        boolean answer = false;
        try
        {
            Matcher matcher = Pattern.compile(REGEX_FOR_QUERY_CREATE_TABLE).matcher(query);
            if (matchQuery(matcher, "CREATE TABLE"))
            {
                if (DATABASE != null)
                {
                    query = formatQuery(query.trim());

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

                    TABLE_NAME = query.substring(0, firstParenthesis).trim().split("\\s+")[2];

                    // Check if table exists or not -----------------------------------------------
                    String metadata_file_path = BASE_DIRECTORY + LOCAL_METADATA_FILE;
                    boolean table_found = false, primary_key=true, foreign_key=true, different_column_names = false;

                    File local_metadata = new File(metadata_file_path);
                    Scanner reader = new Scanner(local_metadata);

                    // Checking for table_name
                    while(reader.hasNextLine()) {
                        String line = reader.nextLine();
                        String[] line_parts = line.split("\\|");

                        if (line_parts[0].equals(DATABASE)) {
                            if (line_parts.length > 1) {

                                String[] table_parts = line_parts[1].trim().split(",");
                                for (String table : table_parts) {
                                    if (table.equals(TABLE_NAME)) {
                                        table_found = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    // Checking all column names are different -----------------------------------------------
                    Set<String> column_names = new HashSet<String>();
                    int column_count = 0;

                    String[] column_parts = query.substring(firstParenthesis+1,lastParenthesis).trim().split(",");
                    for (String s : column_parts) {
                        if (!(s.contains("primary_key") || s.contains("foreign_key"))) {
                            column_count += 1;

                            String[] s_parts = s.trim().split("\\s+");
                            column_names.add(s_parts[0]);
                        }
                    }
                    if (column_names.size() == column_count) {
                        different_column_names = true;
                    }


                    // Checking for primary key -----------------------------------------------
                    if (query.contains("primary key")) {
                        primary_key = false;

                        int first=0, last=0;
                        int start_index = query.indexOf("primary key") + "primary key".length();
                        for (int i=start_index; i<query.length(); i++) {
                            if (Character.compare(query.toCharArray()[i], '(') == 0) {
                                first = i;
                            }
                            if (Character.compare(query.toCharArray()[i], ')') == 0) {
                                last = i;
                                break;
                            }
                        }

                        String pk_column = query.substring(first+1, last).trim();

                        for (String column : column_names) {
                            if (column.equals(pk_column)) {
                                primary_key = true;
                            }
                        }

                    }

                    // Checking for foreign key -----------------------------------------------
                    if (query.contains("foreign key")) {
                        foreign_key = true;
                        String f_string = query.substring(query.indexOf("foreign key") + "foreign key".length()).trim();
                        f_string = f_string.replaceAll("\\s+", "");

                        String main_column="", f_table_name = "", f_column_name = "";

                        // check for main column
                        main_column = f_string.substring(f_string.indexOf("(") + "(".length(), f_string.indexOf(")")).trim();
                        f_string = f_string.substring(f_string.indexOf("references") + "references".length());

                        boolean main_column_found = false, f_table_found=false, f_column_found=false;
                        String column_list = query.substring(query.indexOf("(")).trim();
                        column_list = column_list.substring(1, column_list.length()-1);
                        List<String> c_names = new ArrayList<String>();
                        String[] column_list_parts = column_list.split(",");
                        for (String c : column_list_parts) {
                            c = c.trim();
                            if (!(c.contains("primary key") || c.contains("foreign key"))) {
                                c_names.add(c.split("\\s+")[0].trim());
                            }
                        }
                        for (String c : c_names) {
                            if (main_column.trim().equals(c.trim())) {
                                main_column_found = true;
                            }
                        }
                        if (!main_column_found)
                        {
                            System.out.println("Foreign key column does not match!");
                            return false;
                        }

                        // check for reference table
                        f_table_name = f_string.substring(0, f_string.indexOf("(")).trim();

                        // -- check table & column name
                        String lm_file = BASE_DIRECTORY + LOCAL_METADATA_FILE;
                        String m_file = BASE_DIRECTORY + DATABASE + "/" + DATABASE + "_metadata.txt";

                        // -- checking table name
                        File fl = new File(lm_file);
                        Scanner s = new Scanner(fl);
                        while (s.hasNextLine())
                        {
                            String data = s.nextLine();
                            String[] data_parts = data.split("\\|");
                            if (data_parts[0].trim().equals(DATABASE.trim())) {
                                if (data_parts.length > 1)
                                {
                                    String[] columns = data_parts[1].split(",");
                                    for (String c : columns) {
                                        if (c.equals(f_table_name)) {
                                            f_table_found = true;
                                        }
                                    }
                                }
                                else
                                {
                                    System.out.println("Reference table does not exist!");
                                    return false;
                                }
                            }
                        }

                        if(!f_table_found)
                        {
                            System.out.println("Reference table does not exist!");
                            return false;
                        }


                        // check for reference column
                        f_column_name = f_string.substring(f_string.indexOf("(") + 1, f_string.indexOf(")")).trim();

                        // -- checking column name
                        fl = new File(m_file);
                        s = new Scanner(fl);
                        while (s.hasNextLine()) {
                            String data = s.nextLine();
                            String[] data_parts = data.split("\\|");
                            if (data_parts[0].trim().equals(f_table_name.trim())) {
                                if (data_parts.length > 1) {
                                    String[] column_detail = data_parts[1].split(",");
                                    for (String col : column_detail) {
                                        String cname = col.split("\\s+")[0].trim();
                                        if (cname.equals(f_column_name)) {
                                            f_column_found = true;
                                        }
                                    }
                                }
                            }
                        }

                        if (!f_column_found)
                        {
                            System.out.println("Referenced column does not exist!");
                            return false;
                        }

                    }


                    if (!table_found && primary_key && foreign_key && different_column_names)
                    {
                        answer = true;
                    }
                }
                else {
                    System.out.println("No database selected!");
                    answer = false;
                }

            }
            else {
                System.out.println("Not matching");
                answer = false;
            }

        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
        return answer;
    }

    public boolean parseCreate(String userName, String query)
    {
        boolean answer = false;
        try
        {
            query = query.toLowerCase();
            String[] query_parts = query.split("\\s+");

            if (query_parts.length >= 3)
            {
                String createWhat = query_parts[1];

                switch (createWhat)
                {
                    case "database":
                        answer = parseCreateDatabase(userName, query);
                        break;

                    case "table":
                        answer = parseCreateTable(userName, query);
                        break;

                    default:
                        System.out.println("Can't create - " + createWhat);
                        answer = false;
                }
            }
            else
            {
                System.out.println("Invalid CREATE Query!");
                answer = false;
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
        return answer;
    }

    public boolean parseUse(String userName, String query)
    {
        boolean answer = false;
        try
        {
            Matcher matcher = Pattern.compile(REGEX_FOR_QUERY_USE).matcher(query);
            if (matchQuery(matcher, "USE"))
            {
                query = formatQuery(query.trim());
                String[] query_parts = query.split("\\s+");
                String database = query_parts[1];
                getDatabase();

                for (String db : DATABASES)
                {
                    if (db.equals(database))
                    {
                        return true;
                    }
                }
            }
            DATABASE = null;
            answer = false;
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
        return answer;
    }

    public boolean parseInsert(String userName, String query)
    {
        boolean answer = false;
        try
        {
            Matcher matcher = Pattern.compile(REGEX_FOR_QUERY_INSERT).matcher(query);
            if (matchQuery(matcher, "INSERT")) {
                if (DATABASE != null) {
                    query = formatQuery(query);

                    TABLE_NAME = query.trim().split("\\s+")[2];


                    // Check whether table exists or not
                    boolean table_flag = false;
                    String file_path = BASE_DIRECTORY + LOCAL_METADATA_FILE;
                    File file = new File(file_path);
                    Scanner sc = new Scanner(file);
                    while (sc.hasNextLine()) {
                        String line = sc.nextLine();
                        String[] line_parts = line.split("\\|");
                        if (line_parts[0].equals(DATABASE)) {
                            if (line_parts.length > 1)
                            {
                                String[] tables = line_parts[1].split(",");
                                for (String table : tables) {
                                    if (table.trim().equals(TABLE_NAME.trim())) {
                                        table_flag = true;
                                        break;
                                    }
                                }
                            }
                            else {
                                return false;
                            }

                        }
                    }
                    if (table_flag == false) {
                        System.out.println("Table not found!");
                        return false;
                    }

                    // check value length
                    String value_str = query.substring(query.indexOf("values") + "values".length()).trim();
                    value_str = value_str.substring(1, value_str.length() - 1).trim();

                    int value_len = value_str.split(",").length;
                    int col_len = 0;
                    String column_string = "";

                    String filepath = BASE_DIRECTORY + DATABASE + "/" + DATABASE + "_metadata.txt";
                    File f = new File(filepath);
                    Scanner reader = new Scanner(f);
                    while (reader.hasNextLine()) {
                        String d = reader.nextLine();
                        String[] d_parts = d.split("\\|");
                        if (d_parts[0].trim().equals(TABLE_NAME.trim())) {
                            if (d_parts.length > 1) {
                                column_string = d_parts[1];
                                String[] columns = d_parts[1].split(",");
                                col_len = columns.length;
                                break;
                            } else {
                                System.out.println("No columns");
                                return false;
                            }
                        }
                    }
                    reader.close();

                    if (value_len != col_len) {
                        return false;
                    }

                    // Unique primary key
                    if (column_string.toLowerCase().contains("primary_key")) {
                        column_string = column_string.trim().toLowerCase();

                        int c_number_check = -1;
                        int final_c_number = 0;

                        boolean unique_primary_value = true;
                        String c_name = "";
                        String[] column_details = column_string.split(",");
                        for (String column_detail : column_details) {
                            c_number_check += 1;
                            if (column_detail.contains("primary_key")) {
                                c_name = column_detail.trim().split("\\s+")[0];
                                final_c_number = c_number_check;

                                String values = query.substring(query.indexOf("values") + "values".length());
                                values = values.substring(values.indexOf("(") + "(".length(), values.indexOf(")"));

                                String value = values.trim().split(",")[final_c_number].trim();
                                if (value.contains("'")) {
                                    value = value.trim();
                                    value = value.substring(1, value.length()-1);
                                }

                                String file_p = BASE_DIRECTORY + DATABASE + "/" + TABLE_NAME + ".txt";
                                File fl = new File(file_p);
                                Scanner scanner = new Scanner(fl);
                                while (scanner.hasNextLine()) {
                                    String data = scanner.nextLine();
                                    String[] data_parts = data.split("\\|");

                                    for (String one_value : data_parts) {
                                        if (one_value.trim().equals(value.trim())) {
                                            unique_primary_value = false;
                                            System.out.println("Duplicate primary key value!");
                                            return false;
                                        }
                                    }
                                }
                                scanner.close();
                            }

                        }
                    }

                    // Checking foreign key
                    if (column_string.toLowerCase().contains("foreign_key")) {

                        int column_count=-1, column_number=0;
                        int c_c = -1, c_n=0;

                        column_string = column_string.trim().toLowerCase();
                        String[] column_details = column_string.split(",");
                        for (String column : column_details) {
                            c_c += 1;
                            if (column.contains("foreign_key")) {
                                c_n = c_c;
                                // get table & column name
                                String info = column.substring(column.indexOf("references ") + "references ".length());

                                String main_column = column.split("\\s+")[0].trim();

                                String table_name = info.substring(0, info.indexOf("(")).trim();
                                String column_name = info.substring(info.indexOf("(")+1, info.indexOf(")")).trim();

                                boolean f_table_found=false, f_column_found=false, f_key_value = false;

                                // check table & column name
                                String lm_file = BASE_DIRECTORY + LOCAL_METADATA_FILE;
                                String m_file = BASE_DIRECTORY + DATABASE + "/" + DATABASE + "_metadata.txt";

                                // -- checking table name
                                File fl = new File(lm_file);
                                Scanner s = new Scanner(fl);
                                while (s.hasNextLine()) {
                                    String data = s.nextLine();
                                    String[] data_parts = data.split("\\|");
                                    if (data_parts[0].trim().equals(DATABASE.trim())) {
                                        if (data_parts.length > 1) {
                                            String[] columns = data_parts[1].split(",");
                                            for (String c : columns) {
                                                if (c.equals(table_name)) {
                                                    f_table_found = true;
                                                }
                                            }
                                        } else {
                                            System.out.println("Reference table does not exist!");
                                            return false;
                                        }
                                    }
                                }

                                if(!f_table_found) {
                                    System.out.println("Reference table does not exist!");
                                    return false;
                                }

                                // -- checking column name
                                fl = new File(m_file);
                                s = new Scanner(fl);
                                while (s.hasNextLine()) {
                                    String data = s.nextLine();
                                    String[] data_parts = data.split("\\|");
                                    if (data_parts[0].trim().equals(table_name.trim())) {
                                        if (data_parts.length > 1) {
                                            String[] column_detail = data_parts[1].split(",");
                                            for (String col : column_detail) {
                                                column_count += 1;
                                                String cname = col.split("\\s+")[0].trim();
                                                if (cname.equals(column_name)) {
                                                    column_number = column_count;
                                                    f_column_found = true;
                                                }
                                            }
                                        }
                                    }
                                }

                                if (!f_column_found) {
                                    System.out.println("Referenced column does not exist!");
                                    return false;
                                }

                                // get & check foreign key value
                                String f_file = BASE_DIRECTORY + DATABASE + "/" + table_name + ".txt";

                                // -- get main column value
                                String f_value = value_str.split(",")[c_n];

                                f = new File(f_file);
                                s = new Scanner(f);
                                while (s.hasNextLine()) {
                                    String data = s.nextLine();
                                    String[] data_parts = data.split("\\|");
                                    if (data_parts[column_number].trim().equals(f_value.trim())) {
                                        f_key_value = true;
                                        System.out.println("Value in reference table exists!");
                                        break;
                                    }
                                }

                                if (!f_key_value) {
                                    System.out.println("Value in reference table does not exist!");
                                    return false;
                                }
                                return true;
                            }
                        }
                    }
                    answer = true;
                }
                else {
                    System.out.println("Database not selected!");
                    answer = false;
                }

            }
            else {
                answer = false;
            }

        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
        return answer;
    }

    public boolean parseSelect(String userName, String query)
    {
        boolean answer = false;
        try
        {
            Matcher matcher = Pattern.compile(REGEX_FOR_QUERY_SELECT).matcher(query);
            if (matchQuery(matcher, "SELECT")) {
                if (DATABASE != null) {
                    query = formatQuery(query);

                    if (query.contains("where")) {
                        // WHERE condition check
                        TABLE_NAME = query.substring(0, query.indexOf("where") - 1);
                        TABLE_NAME = TABLE_NAME.substring(TABLE_NAME.indexOf("from") + "from".length()).trim();

                        boolean table_flag = false;
                        String file_path = BASE_DIRECTORY + LOCAL_METADATA_FILE;
                        File file = new File(file_path);
                        Scanner sc = new Scanner(file);
                        while (sc.hasNextLine()) {
                            String line = sc.nextLine();
                            String[] line_parts = line.split("\\|");
                            if (line_parts[0].equals(DATABASE)) {
                                if (line_parts.length > 1) {
                                    String[] tables = line_parts[1].split(",");
                                    for (String table : tables) {
                                        if (table.trim().equals(TABLE_NAME.trim())) {
                                            table_flag = true;
                                            break;
                                        }
                                    }
                                } else {
                                    return false;
                                }

                            }
                        }
                        if (table_flag == false) {
                            System.out.println("Table not found!");
                            return false;
                        }

                        // get where column exists or not
                        String where_column = query.substring(query.indexOf("where") + "where".length());
                        where_column = where_column.substring(0, where_column.indexOf("=")).trim();
                        boolean where_column_found = false;

                        String where_value = query.substring(query.indexOf("=") + "=".length()).trim();
                        if (where_value.contains("'")) {
                            where_value = where_value.trim();
                            where_value = where_value.substring(1, where_value.length()-1);
                        }
                        String filepath = BASE_DIRECTORY + DATABASE + "/" + DATABASE + "_metadata.txt";
                        File f = new File(filepath);
                        Scanner s = new Scanner(f);
                        while (s.hasNextLine()) {
                            String data = s.nextLine();
                            String[] data_parts = data.split("\\|");
                            if (data_parts[0].trim().equals(TABLE_NAME.trim())) {
                                if (data_parts.length > 1) {
                                    String[] cols = data_parts[1].split(",");
                                    for (String c : cols) {
                                        if (c.split("\\s+")[0].trim().equals(where_column.trim())) {
                                            where_column_found = true;
                                            break;
                                        }
                                    }
                                } else {
                                    return false;
                                }
                            }
                        }
                        s.close();

                        if (where_column_found == false) {
                            System.out.println("Column for where condition is not found!");
                            return false;
                        }

                        // Check for * or column names
                        if (!query.contains("*")) {
                            // specific columns
                            String specific_cols = query.substring(query.indexOf("select") + "select".length(), query.indexOf("from")).trim();
                            String[] specific_columns = specific_cols.split(",");

                            String filep = BASE_DIRECTORY + DATABASE + "/" + DATABASE + "_metadata.txt";
                            File fl = new File(filep);
                            Scanner scan = new Scanner(fl);
                            while (scan.hasNextLine()) {
                                String data = scan.nextLine();
                                String[] data_parts = data.split("\\|");
                                if (data_parts[0].equals(TABLE_NAME)) {
                                    if (data_parts.length > 1) {
                                        String[] columns_details = data_parts[1].split(",");

                                        for (String specific_column : specific_columns) {
                                            boolean col_found = false;
                                            for (String c : columns_details) {
                                                String column = c.split("\\s+")[0];
                                                if (column.trim().equals(specific_column.trim())) {
                                                    col_found = true;
                                                }
                                            }
                                            if (col_found == false) {
                                                System.out.println("Column not found!");
                                                return false;
                                            }
                                        }
                                        return true;
                                    } else {
                                        return false;
                                    }
                                }
                            }
                        }
                        // for * condition
                        return true;
                    } else {
                        // NO WHERE condition

                        // Check whether table exists or not
                        TABLE_NAME = query.substring(query.indexOf("from") + "from".length()).trim();

                        boolean table_flag = false;
                        String file_path = BASE_DIRECTORY + LOCAL_METADATA_FILE;
                        File file = new File(file_path);
                        Scanner sc = new Scanner(file);
                        while (sc.hasNextLine()) {
                            String line = sc.nextLine();
                            String[] line_parts = line.split("\\|");
                            if (line_parts[0].equals(DATABASE)) {
                                if (line_parts.length > 1) {
                                    String[] tables = line_parts[1].split(",");
                                    for (String table : tables) {
                                        if (table.trim().equals(TABLE_NAME.trim())) {
                                            table_flag = true;
                                            break;
                                        }
                                    }
                                } else {
                                    return false;
                                }
                            }
                        }
                        if (table_flag == false) {
                            System.out.println("Table not found!");
                            return false;
                        }

                        // Checking * or specific columns
                        if (!query.contains("*")) {
                            // Specific columns
                            String specific_cols = query.substring(query.indexOf("select") + "select".length(), query.indexOf("from")).trim();
                            String[] specific_columns = specific_cols.split(",");

                            String filep = BASE_DIRECTORY + DATABASE + "/" + DATABASE + "_metadata.txt";
                            File fl = new File(filep);
                            Scanner scan = new Scanner(fl);
                            while (scan.hasNextLine()) {
                                String data = scan.nextLine();
                                String[] data_parts = data.split("\\|");
                                if (data_parts[0].equals(TABLE_NAME)) {
                                    if (data_parts.length > 1) {
                                        String[] columns_details = data_parts[1].split(",");

                                        for (String specific_column : specific_columns) {
                                            boolean col_found = false;
                                            for (String c : columns_details) {
                                                String column = c.split("\\s+")[0];
                                                if (column.trim().equals(specific_column.trim())) {
                                                    col_found = true;
                                                }
                                            }
                                            if (col_found == false) {
                                                System.out.println("Column not found!");
                                                return false;
                                            }
                                        }
                                        return true;
                                    } else {
                                        return false;
                                    }
                                }
                            }
                        }
                        // For * condition
                        answer = true;
                    }
                } else {
                    System.out.println("Database not selected!");
                    answer = false;
                }
            }
            else {
                answer = false;
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
        return answer;
    }

    public boolean parseUpdate(String userName, String query)
    {
        boolean answer = false;
        try
        {
            Matcher matcher = Pattern.compile(REGEX_FOR_QUERY_UPDATE).matcher(query);
            if (matchQuery(matcher, "UPDATE")) {
                if (DATABASE != null) {
                    query = formatQuery(query);

                    // Check tablename
                    TABLE_NAME = query.split("\\s+")[1];
                    boolean table_found = false;

                    String file_path = BASE_DIRECTORY + LOCAL_METADATA_FILE;
                    File f = new File(file_path);
                    Scanner sc = new Scanner(f);
                    while (sc.hasNextLine()) {
                        String data = sc.nextLine();
                        String[] data_parts = data.split("\\|");
                        if (data_parts[0].trim().equals(DATABASE.trim())) {
                            if (data_parts.length > 1) {
                                String cols = data_parts[1];
                                String[] cols_names = cols.split(",");

                                for (String column : cols_names) {
                                    if (column.trim().equals(TABLE_NAME.trim())) {
                                        table_found = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    answer = table_found;
                }
                else
                {
                    System.out.println("Database not selected!");
                    answer = false;
                }
            }
            else
            {
                answer = false;
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
        return answer;
    }

    public boolean parseDelete(String userName, String query)
    {
        boolean answer = false;
        try
        {
            Matcher matcher = Pattern.compile(REGEX_FOR_QUERY_DELETE).matcher(query);
            if (matchQuery(matcher, "DELETE"))
            {
                if (DATABASE != null) {
                    query = formatQuery(query.trim());

                    TABLE_NAME = query.substring(query.indexOf("from ") + "from ".length());
                    TABLE_NAME = TABLE_NAME.substring(0, TABLE_NAME.indexOf(" where"));

                    String column, value;
                    column = query.substring(query.indexOf("where ") + "where ".length());
                    column = column.substring(0, column.indexOf("=")).trim();
                    value = query.substring(query.indexOf("=") + "=".length()).trim();

                    boolean table_exist=false, column_exist=false, value_datatype=false;

                    // Check whether table exists or not
                    String metadata_file_path = BASE_DIRECTORY + LOCAL_METADATA_FILE;

                    File local_metadata = new File(metadata_file_path);
                    Scanner reader = new Scanner(local_metadata);

                    // Checking for table_name
                    while(reader.hasNextLine()) {
                        String line = reader.nextLine();
                        String[] line_parts = line.split("\\|");

                        if (line_parts[0].equals(DATABASE)) {
                            if (line_parts.length > 1) {

                                String[] table_parts = line_parts[1].trim().split(",");
                                for (String table : table_parts) {

                                    if (table.trim().equals(TABLE_NAME.trim())) {
                                        table_exist = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    answer = table_exist;
                }
                else
                {
                    System.out.println("Database not selected.");
                    answer = false;
                }
            }
            else
            {
                answer = false;
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
        return answer;
    }

    public boolean parseStartTransaction(String userName, String query) {
        Matcher matcher = Pattern.compile(REGEX_FOR_START_TRANSACTION).matcher(query);
        if (matchQuery(matcher, "START TRANSACTION")) {
            return true;
        } else {
            System.out.println("Invalid query!");
            return false;
        }

    }

    public boolean parseCommitTransaction(String userName, String query) {
        Matcher matcher = Pattern.compile(REGEX_FOR_COMMIT_TRANSACTION).matcher(query);
        if (matchQuery(matcher, "COMMIT")) {
            return true;
        } else {
            System.out.println("Invalid query!");
            return false;
        }

    }

    public boolean parseRollbackTransaction(String userName, String query) {
        Matcher matcher = Pattern.compile(REGEX_FOR_ROLLBACK_TRANSACTION).matcher(query);
        if (matchQuery(matcher, "ROLLBACK")) {
            return true;
        } else {
            System.out.println("Invalid query!");
            return false;
        }

    }

    public boolean parseQuery(String userName, String query)
    {
        query = query.toLowerCase();
        String queryType = query.split("\\s+")[0];

        switch (queryType)
        {
            case "create":
                return parseCreate(userName, query);

            case "use":
                return parseUse(userName, query);

            case "insert":
                return parseInsert(userName, query);

            case "select":
                return parseSelect(userName, query);

            case "update":
                return parseUpdate(userName, query);

            case "delete":
                return parseDelete(userName, query);

            case "start":
                if(parseStartTransaction(userName, query)) {
                    Transaction transaction = new Transaction();
                    try
                    {
                        transaction.startTransaction(userName);
                    }
                    catch (Exception e)
                    {
                        e.printStackTrace();
                        logger.crashReport(e);
                    }
                }
                return false;

            case "commit":
                if(!parseCommitTransaction(userName, query)) {
                    System.out.println("Invalid query!");
                    return false;
                }
                logger.transactionLog("Tranaction committed successfully!");

            case "rollback":
                if(!parseRollbackTransaction(userName, query)) {
                    System.out.println("Invalid query!");
                    return false;
                }
                logger.transactionLog("Tranaction rollback successfully!");

                isTransaction = false;
                return false;

            default:
                System.out.println("Invalid Query Type! - " + queryType.toUpperCase());
                return false;
        }
    }
}
