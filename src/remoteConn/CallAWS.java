package remoteConn;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

public class CallAWS {
	private static final long serialVersionUID = 1L;
	private JSch jschSSHChannel;
	private ChannelSftp channelSftp;
	private String strUserName;
	private String strConnectionIP;
	private int intConnectionPort;
	private String strPassword;
	private Session sesConnection;
	private int intTimeOut;

	public static void main(String args[]) {
		try {
			ArrayList<String> userIdList = new ArrayList<String>();
			CallAWS test = new CallAWS();
			// get user ids
			//temp comment
			userIdList = test.getUserIds();
			// transfer user id file to AWS
			//temp comment
			test.writeToAWS(userIdList);
			// call stage 1
			// to -do

			// get hashtag file as input for stage 2
			// taking temp File for now
			// temp file is /home/ubuntu/new_tags.txt

			// call stage 3 taking hastag file as input
			test.callStage2();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * to get user ids from MySQL data base. Writter to retrieve only 20 user
	 * ids for the time being
	 * 
	 * @return ArrayList<String> user ids
	 */
	public ArrayList<String> getUserIds() {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		ArrayList<String> userIDList = new ArrayList<String>();
		String driver = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://localhost:3306/workspace";
		try {
			Class.forName(driver).newInstance();
			conn = DriverManager.getConnection(url, "basma", "Sirius@95");
			String query = "select distinct user_name from boston_bombing order by created_date limit 20;";
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			while (rs.next()) {
				userIDList.add(rs.getString(1));
				System.out.println(rs.getString(1));
			}
		} catch (Exception e) {
			e.printStackTrace();

		} finally {
			return userIDList;
		}

	}

	/**
	 * create file from arraylist of user ids and transfer file to AWS as input
	 * for state 1
	 * 
	 * @param userIdList
	 */

	public void writeToAWS(ArrayList<String> userIdList) {
		connectToJsch();
		try {
			// create local file
			File f = new File("C:\\topsy\\userIDFile.txt");
			if (!f.exists()) {
				f.createNewFile();
			}
			FileWriter fWriter = new FileWriter(f.getAbsoluteFile());
			BufferedWriter bWriter = new BufferedWriter(fWriter);
			for (int i = 0; i < userIdList.size(); i++) {
				String content = userIdList.get(i) + "\n";
				if (i == 1)
					bWriter.write(content);
				else
					bWriter.append(content);
			}
			bWriter.close();
			Channel channel = sesConnection.openChannel("sftp");
			channel.connect();
			channelSftp = (ChannelSftp) channel;
			channelSftp.cd("/home/ubuntu/");
			channelSftp.put(new FileInputStream(f), f.getName());
			channelSftp.exit();
			channel.disconnect();
			//sesConnection.disconnect();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * connect to jsch for transferring file
	 */
	public void connectToJsch() {
		System.out.println("reached connect");
		jschSSHChannel = new JSch();
		strConnectionIP = "ec2-50-16-57-196.compute-1.amazonaws.com";
		int intConnectionPort = 22;
		try {
			System.out.println("trying to connect");
			sesConnection = jschSSHChannel.getSession("ubuntu",
					strConnectionIP, 22);
			// sesConnection.setPassword(strPassword);
			java.util.Properties config = new java.util.Properties();
			config.put("StrictHostKeyChecking", "no");
			sesConnection.setConfig(config);
			jschSSHChannel.addIdentity("C:\\key\\HadoopKey.pem");
			sesConnection.connect();
			System.out.println("connecting");
		} catch (Exception e) {
			System.out.println("exception 1");
			e.printStackTrace();
		}
	}

	/**
	 * executing commands in remote AWS
	 * 
	 * @return
	 */
	String sendCommand() {
		StringBuilder outputBuffer = new StringBuilder();
		try {
			System.out.println("send command");
			Channel channel = sesConnection.openChannel("exec");
			String command = "pwd";
			((ChannelExec) channel).setCommand(command);
			channel.connect();
			InputStream commandOutput = channel.getInputStream();
			int readByte = commandOutput.read();
			while (channel.isEOF()) {
				System.out.println("a");
				outputBuffer.append((char) readByte);
				readByte = commandOutput.read();
				System.out.println(readByte);
			}
			System.out.println(readByte);
			channel.disconnect();
		} catch (Exception e) {
			System.out.println("exception 2");
			e.printStackTrace();
		}
		return outputBuffer.toString();
	}

	public void callStage2() {
		StringBuilder outputBuffer = new StringBuilder();
		try {
			System.out.println("calling stage 2");
			Channel channel = sesConnection.openChannel("exec");
			String command = "/home/ubuntu/Stage2.sh";
			/*String command1 = "export HADOOP_CLASSPATH=/home/ubuntu/twitter4j-core-3.0.3.jar:/home/ubuntu/hive-serdes-1.0-SNAPSHOT.jar:/home/ubuntu/lib/jsch-0.1.50.jar:/home/ubuntu/lib/libfb303-0.9.0.jar:/home/ubuntu/lib/logback-classic-1.0.0.jar:/home/ubuntu/lib/logback-core-1.0.0.jar:/home/ubuntu/lib/slf4j-api-1.6.1.jar:/home/ubuntu/lib/xuggle-xuggler-5.4.jar:/home/ubuntu/lib/derby-10.4.2.0.jar:/home/ubuntu/lib/antlr-runtime-3.0.1.jar:/home/ubuntu/lib/servlet-api-2.5-20081211.jar:/home/ubuntu/lib/commons-logging-1.0.4.jar:/home/ubuntu/lib/slf4j-api-1.7.5.jar:/home/ubuntu/lib/apache-logging-log4j.jar:/home/ubuntu/lib/apache-log4j-1.2.15.jar:/home/ubuntu/lib/hive-jdbc-0.10.0.jar:/home/ubuntu/lib/hive-metastore-0.10.0.jar:/home/ubuntu/lib/hadoop-0.20.0-core.jar:/home/ubuntu/lib/jdo2-api-2.3-ec.jar:/home/ubuntu/lib/hive-exec-0.10.0.jar:/home/ubuntu/lib/hive-service-0.10.0.jar:/home/ubuntu/lib/jpox-rdbms-1.2.2.jar:/home/ubuntu/lib/jpox-core-1.2.2.jar:/home/ubuntu/lib/mysql-connector-java-5.1.18-bin.jar\n";
			String command2 = "export LIBJARS=/home/ubuntu/twitter4j-core-3.0.3.jar,/home/ubuntu/hive-serdes-1.0-SNAPSHOT.jar,/home/ubuntu/lib/jsch-0.1.50.jar,/home/ubuntu/lib/libfb303-0.9.0.jar,/home/ubuntu/lib/logback-classic-1.0.0.jar,/home/ubuntu/lib/logback-core-1.0.0.jar,/home/ubuntu/lib/slf4j-api-1.6.1.jar,/home/ubuntu/lib/xuggle-xuggler-5.4.jar,/home/ubuntu/lib/derby-10.4.2.0.jar,/home/ubuntu/lib/antlr-runtime-3.0.1.jar,/home/ubuntu/lib/servlet-api-2.5-20081211.jar,/home/ubuntu/lib/commons-logging-1.0.4.jar,/home/ubuntu/lib/slf4j-api-1.7.5.jar,/home/ubuntu/lib/apache-logging-log4j.jar,/home/ubuntu/lib/apache-log4j-1.2.15.jar,/home/ubuntu/lib/hive-jdbc-0.10.0.jar,/home/ubuntu/lib/hive-metastore-0.10.0.jar,/home/ubuntu/lib/hadoop-0.20.0-core.jar,/home/ubuntu/lib/jdo2-api-2.3-ec.jar,/home/ubuntu/lib/hive-exec-0.10.0.jar,/home/ubuntu/lib/hive-service-0.10.0.jar,/home/ubuntu/lib/jpox-rdbms-1.2.2.jar,/home/ubuntu/lib/jpox-core-1.2.2.jar,/home/ubuntu/lib/mysql-connector-java-5.1.18-bin.jar\n";
			String command3 = "/home/ubuntu/hadoop/bin/hadoop jar /home/ubuntu/tweetCollector1.jar TaskSplitter -libjars ${LIBJARS} /home/ubuntu/tags.txt /home/ubuntu/output12\n";
			
			InputStream is = new ByteArrayInputStream(command.getBytes());
			channel.setInputStream(is);
			channel.setOutputStream(System.out, true);
			channel.connect();
			Thread.sleep(3000);
			 */
			((ChannelExec)channel).setCommand(command);
			((ChannelExec)channel).setErrStream(System.err);
			BufferedReader in=new BufferedReader(new InputStreamReader(channel.getInputStream()));
			channel.connect();
			String msg = null;
			while ((msg=in.readLine()) != null) {
				//System.out.println("a");
				System.out.println(msg);
			}
	             channel.disconnect();
	             sesConnection.disconnect();
		}catch(Exception e){
			e.printStackTrace();
		}
}
}