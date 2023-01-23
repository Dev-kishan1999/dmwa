package userinterface;
import logmanagement.LogManagement;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;

public class Hash
{
    static MessageDigest md;
    LogManagement logger = new LogManagement();

    static
    {
        try
        {
            md = MessageDigest.getInstance("MD5");
        }
        catch (NoSuchAlgorithmException e)
        {
            e.printStackTrace();
        }
    }

    public static String userID(String username)
    {
        return getString(username);
    }

    public static String getString(String username)
    {
        md.update(username.getBytes());
        byte[] digest = md.digest();
        BigInteger no = new BigInteger(1, digest);
        String hashText = no.toString(16);
        while(hashText.length() < 32 )
        {
            hashText = "0" + hashText;
        }
        return hashText;
    }

    public static String password(String password)
    {
        return getString(password);
    }
}
