package Connection;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import java.io.ByteArrayOutputStream;

public class SSHConnection {

    public void connect(String query) throws Exception {

        System.out.println("query in connect is : "+query);
        listFolderStructure("gcpKey", "123qwe", "34.136.171.220", 22, query );

    }
    public void listFolderStructure(String username, String password,
                                           String host, int port, String command) throws Exception {

        System.out.println("query in listFolderStructure is : "+command);
        JSch jsch=null;
        Session session = null;
        ChannelExec channel = null;

        try {
            jsch=new JSch();
            jsch.setKnownHosts("C:\\Users\\shara\\.ssh/known_hosts");
            jsch.addIdentity("C:\\Users\\shara\\.ssh/id_rsa");
            session = jsch.getSession(username, host, port);
            session.setPassword(password);
            session.setConfig("StrictHostKeyChecking", "no");

            session.connect();

            channel = (ChannelExec) session.openChannel("exec");
            channel.setCommand(command);
            ByteArrayOutputStream responseStream = new ByteArrayOutputStream();
            channel.setOutputStream(responseStream);
            channel.connect();

            while (channel.isConnected()) {
                Thread.sleep(100);
            }

            String responseString = new String(responseStream.toByteArray());
            System.out.println(responseString);
        } finally {
            if (session != null) {
                session.disconnect();
            }
            if (channel != null) {
                channel.disconnect();
            }
        }
    }
}
