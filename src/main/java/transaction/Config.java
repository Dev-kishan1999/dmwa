package transaction;

import java.util.Random;

public class Config {
    public static String[] DDL = new String[]{"select","create"};
    public static String[] DML = new String[]{"insert","update","delete"};
    public static int UPPERBOUND = 10000;
    public static String idGenerator(){
        Random random = new Random();
        return "T"+random.nextInt(UPPERBOUND);
    }
}
