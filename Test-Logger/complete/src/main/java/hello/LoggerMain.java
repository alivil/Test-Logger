package hello;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hsqldb.server.Server;
import org.json.JSONObject;

public class LoggerMain {
  static final Log logger = LogFactory.getLog(LoggerMain.class.getName());
  
  public static void main(String[] args) {
    File file = new File(args[0]);
    HashMap<Object, Object> startMap = new HashMap<>();
    HashMap<Object, Object> endMap = new HashMap<>();
    HashSet<String> uniqueSet = new HashSet();
    try {
      FileReader fr = new FileReader(file);
      BufferedReader br = new BufferedReader(fr);
      try {
        String line;
        while ((line = br.readLine()) != null) {
          JSONObject json = new JSONObject(line);
          String logId = json.getString("id");
          uniqueSet.add(logId);
          String[] s = line.split(":");
          if (line.contains("STARTED")) {
            startMap.put(logId, line);
            continue;
          } 
          if (line.contains("FINISHED"))
            endMap.put(logId, line); 
        } 
        connectHsqlDB();
        for (String skey : uniqueSet) {
          String jsonStartReq = (String)startMap.get(skey);
          String jsonendRes = (String)endMap.get(skey);
          JSONObject json1 = new JSONObject(jsonStartReq);
          JSONObject json2 = new JSONObject(jsonendRes);
          String[] keyname = JSONObject.getNames(json1);
          String eventType = null;
          String eventHost = null;
          byte b;
          int i;
          String[] arrayOfString1;
          for (i = (arrayOfString1 = keyname).length, b = 0; b < i; ) {
            String key = arrayOfString1[b];
            if ("type".equalsIgnoreCase(key)) {
              eventType = json1.getString("type");
            } else if ("host".equalsIgnoreCase(key)) {
              eventHost = json1.getString("host");
            } 
            b++;
          } 
          System.out.println(keyname.length);
          System.out.println(keyname[0]);
          System.out.println(keyname[1]);
          System.out.println(keyname[2]);
          Long req = Long.valueOf(json1.getLong("timestamp"));
          Long res = Long.valueOf(json2.getLong("timestamp"));
          Long duration = Long.valueOf(res.longValue() - req.longValue());
          String eventAlert = "false";
          if (duration.longValue() > 4L)
            eventAlert = "true"; 
          insertDb(json1.getString("id"), duration.longValue(), eventType, eventHost, eventAlert);
        } 
        fetchRecordCount();
      } catch (IOException e) {
        e.printStackTrace();
      } 
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } 
  }
  
  static void connectHsqlDB() {
    Server server = new Server();
    server.setDatabaseName(0, "tmpDb");
    server.setDatabasePath(0, "mem:tempDb");
    server.setPort(9001);
    server.start();
    logger.info("HSQL DB Server Started");
    String url = "jdbc:hsqldb:hsql://localhost:9001/tmpDb; mem:tmpDb;";
    String user = "SA";
    String pass = "";
    String query = "CREATE TABLE CSLOGS (\n    EventId varchar(50),\n    EventDuration int,\n    Type varchar(255),\n    Host varchar(100),\n    Alert varchar(10) \n);";
    logger.debug("Create logging table query :" + query);
    Connection con = null;
    Statement st = null;
    try {
      con = DriverManager.getConnection(url, user, pass);
      st = con.createStatement();
      st.executeUpdate(query);
      logger.info("Created Table CSLOGS");
      st.close();
      con.close();
    } catch (SQLException e) {
      logger.warn("LoggerMain:connectHsqlDB:SQLException");
      e.printStackTrace();
    } 
  }
  
  public static void insertDb(String eventId, long eventDuartion, String type, String host, String alert) {
    try {
      String url = "jdbc:hsqldb:hsql://localhost:9001/tmpDb; mem:tmpDb;";
      String user = "SA";
      String pass = "";
      Connection con = DriverManager.getConnection(url, user, pass);
      Statement st = con.createStatement();
      System.out.println(eventId);
      String query = "INSERT INTO CSLOGS (EventId, EventDuration, Type, Host, Alert)\nVALUES (?, ?, ?, ?, ?);";
      PreparedStatement pst = con.prepareStatement(query);
      pst.setString(1, eventId);
      pst.setLong(2, eventDuartion);
      pst.setString(3, type);
      pst.setString(4, host);
      pst.setString(5, alert);
      pst.executeUpdate();
      logger.info("Insert record into CSLOGS table : EventId=" + eventId + " EventDuartion(ms)=" + eventDuartion + ", Type=" + type + ", Host=" + host + ", Alert=" + alert);
      st.close();
      pst.close();
      con.close();
    } catch (Exception e) {
      e.printStackTrace();
    } 
  }
  
  public static void fetchRecordCount() {
    try {
      String url = "jdbc:hsqldb:hsql://localhost:9001/tmpDb; mem:tmpDb;";
      String user = "SA";
      String pass = "";
      Connection con = DriverManager.getConnection(url, user, pass);
      Statement st = con.createStatement();
      ResultSet resultSet = st.executeQuery("SELECT count(*) from CSLOGS");
      while (resultSet.next())
        logger.info("Total records inserted in CSLOGS table :" + resultSet.getLong(1)); 
      resultSet.close();
      st.close();
      con.close();
    } catch (Exception e) {
      e.printStackTrace();
    } 
  }
}
