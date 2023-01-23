package userinterface;
import DDBMS.D2_DB;
import logmanagement.LogManagement;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Scanner;

public class Login
{
    static Scanner sc = new Scanner(System.in);
    Menu menu = new Menu();
    Registration register = new Registration();
    LogManagement logger = new LogManagement();

    public void loginUser()
    {
        try
        {
            System.out.print("Please enter your username: ");
            String realUserName = sc.nextLine();
            String toCheckUser = Hash.userID(realUserName);
            if (register.checkUser(toCheckUser))
            {
                System.out.println("User exists in the database.");
                System.out.print("Please enter your password: ");
                String realPassword = sc.nextLine();
                String toCheckPass = Hash.password(realPassword);
                if (validateUser(toCheckUser, toCheckPass))
                {
                    System.out.println("Password is correct.");
                    if (checkSecAns(toCheckUser))
                    {
                        System.out.println("User logged in successfully!");
                        menu.loginMenu(realUserName);
                    }
                    else
                    {
                        System.out.println("Security answer is incorrect. Retry logging in again!");
                    }
                }
                else
                {
                    System.out.println("Username and password doesn't match. Retry again!");
                }
            }
            else
            {
                System.out.println("User doesn't exist in the database. Please register first!");
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.crashReport(e);
        }
    }

    public boolean validateUser(String username, String password)
    {
        boolean isValid = false;
        try
        {
            String eachLine = "";
            BufferedReader br = new BufferedReader(new FileReader(System.getProperty("user.dir") +
                    "/" + D2_DB.VIRTUAL_MACHINE + "/User_Profile.txt"));
            while((eachLine = br.readLine()) != null)
            {
                String[] allLines = eachLine.split("\n");
                for (String everyLine : allLines)
                {
                    String[] values = everyLine.split("\\|");
                    if(values[0].equals(username))
                    {
                        if(values[1].equals(password))
                        {
                            isValid = true;
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
        return isValid;
    }

    public boolean checkSecAns(String username)
    {
        boolean isValid = false;
        String eachLine = "";
        try
        {
            BufferedReader br = new BufferedReader(new FileReader(System.getProperty("user.dir") +
                    "/" + D2_DB.VIRTUAL_MACHINE + "/User_Profile.txt"));
            while((eachLine = br.readLine()) != null)
            {
                String[] allLines = eachLine.split("\n");
                for(String everyLine : allLines)
                {
                    String[] values = everyLine.split("\\|");
                    if(values[0].equals(username))
                    {
                        System.out.println("Your security questions is: '" + values[2] + "'");
                        System.out.print("Please enter the answer to your security question: ");
                        String toCheckAns = sc.nextLine();
                        if(values[3].equals(toCheckAns))
                        {
                            isValid = true;
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
        return isValid;
    }
}
