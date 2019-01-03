import java.sql.*;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class n00946743_Assign4 {
   public static void main(String[] args){
      try{
         String DATABASE_DRIVER = "com.mysql.cj.jdbc.Driver";
         String DATABASE_URL = "jdbc:mysql://localhost:3306/mysql";
         String USERNAME = "root";
         String PASSWORD = "toor";
         String DATABASE_NAME = "PlayerDB_Assign4";
         Connection myConn = DriverManager.getConnection(DATABASE_URL, USERNAME, PASSWORD);
         Statement myStmt = myConn.createStatement();
         
         //Create Database and use it
         myStmt.executeUpdate("CREATE DATABASE IF NOT EXISTS " + DATABASE_NAME);         
         myStmt.executeQuery("USE " + DATABASE_NAME);         
         
         //Creating "Table" objects for each data set
         String[] playerargs = {"player_id","tag","real_name","nationality","birthday","game_race"};
         String[] playertypes = {"INTEGER","VARCHAR(255)","VARCHAR(255)","VARCHAR(255)","DATE","VARCHAR(255)"};
         String[][] playerspec = {};
         String playerkey = "player_id";
         Table players = new Table("players", playerargs, playertypes, playerspec, playerkey);
         
         String[] teamargs = {"team_id","name","founded","disbanded"};
         String[] teamtypes = {"INTEGER","VARCHAR(255)","DATE","DATE"};
         String[][] teamspec = {};
         String teamkey = "team_id";
         Table teams = new Table("teams", teamargs, teamtypes, teamspec, teamkey);

         String[] memberargs = {"player","team","start_date","end_date"};
         String[] membertypes = {"INTEGER","INTEGER","DATE","DATE"};
         String[][] memberspec = {{"player", "players","player_id"},{"team", "teams","team_id"}};
         String memberkey = "";
         Table members = new Table("members", memberargs, membertypes, memberspec, memberkey);
         
         String[] tournamentargs = {"tournament_id","name","region","major"};
         String[] tournamenttypes = {"INTEGER","VARCHAR(255)","VARCHAR(255)","BOOLEAN"};
         String[][] tournamentspec = {};
         String tournamentkey = "tournament_id";
         Table tournaments = new Table("tournaments", tournamentargs, tournamenttypes,tournamentspec, tournamentkey);
         
         String[] matchargs = {"match_id","date","tournament","playerA", "playerB", "scoreA", "scoreB", "offline"};
         String[] matchtypes = {"INTEGER","DATE","INTEGER","INTEGER","INTEGER","INTEGER","INTEGER","BOOLEAN"};
         String[][] matchspec = {{"tournament", "tournaments","tournament_id"},{"playerA", "players","player_id"},{"playerB", "players","player_id"}};
         String matchkey = "match_id";
         Table matches = new Table("matches_v2", matchargs, matchtypes, matchspec, matchkey);
         
         String[] earningargs = {"tournament","player","prize_money","position"};
         String[] earningtypes = {"INTEGER","INTEGER","INTEGER","INTEGER"};
         String[][] earningspec = {{"tournament", "tournaments","tournament_id"},{"player", "players","player_id"}};
         String earningkey = "";
         Table earnings = new Table("earnings", earningargs, earningtypes, earningspec, earningkey);
         
         Table[] tables = {players, teams, members, tournaments, matches, earnings};
         
         //CREATE TABLES
         for (int i=0;i<tables.length;i++){
            myStmt.executeUpdate(tables[i].initializeTable());
         }

         //CONFIRM TABLES CREATED CORRECTLY
         /*for (int i=0;i<tables.length;i++){
            ResultSet myRS = myStmt.executeQuery("SELECT * FROM " + tables[i].name);
            ResultSetMetaData rsmd = myRS.getMetaData();
            System.out.println("Number of columns in " + tables[i].name + ": " + rsmd.getColumnCount());
            for (int j=1;j<=rsmd.getColumnCount();j++){
               System.out.println("Column " + Integer.toString(j) + " Name: " + rsmd.getColumnName(j));
               System.out.println("Column " + Integer.toString(j) + " Type: " + rsmd.getColumnTypeName(j));
            }
            System.out.println(" ");
         }*/
         
         //READING EACH CSV QUICKLY TAKING NOTE OF HOW MANY LINES EACH IS FOR THE PROGRESS BAR
         for (int i=0;i<tables.length;i++){
            try{
               BufferedReader reader = new BufferedReader(new FileReader(tables[i].name + ".csv"));
               int lines = 0;
               while (reader.readLine() != null) lines++;
               reader.close();
               tables[i].linecount = lines;
            } catch(FileNotFoundException e) {} catch(IOException e) {}
         }
         //READING CSV FILES AND INSERTING INTO DATABASE
         
         for (int i=0;i<tables.length;i++){
            String filename = tables[i].name + ".csv";
            BufferedReader br = null;
            String line = "";
            int tempint = 0;
            int prog = 0;
            int progcheck = 1;
            System.out.println("Inserting values into table: " + tables[i].name);
            try {
               br = new BufferedReader(new FileReader(filename));
               while ((line = br.readLine()) != null){
                  prog = (int)(tempint * 100.0f)/tables[i].linecount;
                  if (prog > progcheck){
                     System.out.print(".");
                     progcheck += 1;
                  }
                  tempint += 1;
                  String[] item = line.split(",");
                  //Inserting the values into the table
                  myStmt.executeUpdate(tables[i].insertValues(item));
               }
               br.close();
               System.out.println("");
            } catch (FileNotFoundException e) {
               e.printStackTrace();
            } catch (IOException e) {
               e.printStackTrace();
            }
         }       

         Scanner scanner = new Scanner(System.in);
         
         //QUERY TIME
         while(true){
            try{
               System.out.println("Please select an option:\n1) Find players by birth year and month\n2) Add player to team");
               System.out.println("3) Show listing of names and birthdays for a given nationality\n4) Show all triple crown winners");
               System.out.println("5) Show former ROOT Gaming team members\n0)Exit");
               int input = scanner.nextInt();
               switch (input){
                  case 1:
                     System.out.println("Input players birth month:");
                     int bmonth = scanner.nextInt();
                     System.out.println("Input players birth year:");
                     int byear = scanner.nextInt();
                     ResultSet resultSet = myStmt.executeQuery(queryBday(bmonth, byear));
                     printResultSet(resultSet);
                     break;
                  case 2:
                     System.out.println("Input player id:");
                     int pid = scanner.nextInt();
                     System.out.println("Input team id:");
                     int tid = scanner.nextInt();
                     queryToTeam(myStmt, pid, tid);
                     break;
                  case 3:
                     System.out.println("Input nationality:");
                     String nat = scanner.next();
                     queryNationality(myStmt, nat);                  
                     break;
                  case 4:
                     System.out.println("Checking Triple Crown Winners...");
                     ResultSet resultSet1 = myStmt.executeQuery("SELECT tag, game_race FROM players where player_id in (SELECT player FROM earnings WHERE player in (SELECT player FROM earnings WHERE tournament in (SELECT tournament_id FROM tournaments WHERE region = 'EU' AND major = true)) AND player in (SELECT player FROM earnings WHERE tournament in (SELECT tournament_id FROM tournaments WHERE region = 'KR' AND major = true)) AND player in (SELECT player FROM earnings WHERE tournament in (SELECT tournament_id FROM tournaments WHERE region = 'AM' AND major = true)))");
                     printResultSet(resultSet1);
                     break;
                  case 5:
                     System.out.println("Checking former ROOT Gaming team members...");
                     ResultSet resultSet2 = myStmt.executeQuery("SELECT tag, real_name, end_date FROM players LEFT JOIN members ON players.player_id = members.player where player in (SELECT player FROM members WHERE team in (SELECT team_id from teams where name = 'ROOT Gaming') and end_date IS NOT NULL and player NOT IN (SELECT player FROM members WHERE team in (SELECT team_id FROM teams WHERE name = 'ROOT Gaming') AND end_date IS NULL)) group by tag");
                     printResultSet(resultSet2);
                     break;
                  case 0:
                     //Exit and drop database
                     myStmt.executeUpdate("DROP DATABASE " + DATABASE_NAME);
                     System.exit(0);
                     break;
                  default:
                     System.out.println("Yikes! Looks like that's an invalid input! Try again!");
               
               }
            } catch (Exception e){
               System.out.println("Uh oh! Something went wrong! Let's try that again!");
               e.printStackTrace();
            }
         }

       } catch(SQLException e){
         e.printStackTrace();
      }
   }

   public static String queryBday(int bmonth, int byear){
      String output = "SELECT * FROM players WHERE YEAR(birthday) = " + byear + " AND MONTH(birthday) = " + bmonth;
      return output;
   }
   
   public static void queryToTeam(Statement myStmt, int pid, int tid) throws SQLException{
      ResultSet resultSet = myStmt.executeQuery("SELECT * FROM members WHERE player = " + pid + " AND team = " + tid);
      if (resultSet.next()) {
      //Already on the team
         return;
      } else {
         resultSet = myStmt.executeQuery("SELECT * FROM members WHERE player = " + pid + " AND end_date IS NULL");
         if (resultSet.next()){
            //On a different team
            myStmt.executeUpdate("UPDATE members SET end_date = CURDATE() WHERE player = " + pid + " AND end_date IS NULL");
         }
         myStmt.executeUpdate("INSERT INTO members VALUES (" + pid + ", " + tid + ", CURDATE(), NULL)");
      }
   }
   
   public static void queryNationality(Statement myStmt, String nat) throws SQLException{
      ResultSet resultSet = myStmt.executeQuery("SELECT * FROM players WHERE nationality = '" + nat + "'");
      System.out.println(padRight("Name",21) + padRight("Birthday",21));
      while (resultSet.next()){
         String name = resultSet.getString(3);
         String birth = resultSet.getString(5);
         System.out.println(padRight(name,20) + padRight(birth,20));
      }
   }
   
   //A function that prints a given result set object
   public static void printResultSet(ResultSet resultSet) throws SQLException{
      ResultSetMetaData rsmd = resultSet.getMetaData();
      int columnsNumber = rsmd.getColumnCount();
      for (int i=1;i<=columnsNumber;i++){
         System.out.print(padRight(rsmd.getColumnName(i),20));
      }
      System.out.println("");

      while (resultSet.next()) {
          for (int i = 1; i <= columnsNumber; i++) {
              String columnValue = resultSet.getString(i);
              System.out.print(padRight(columnValue,20));
          }
          System.out.println("");
      }
   }
   
   //A simple function to format strings
   public static String padRight(String s, int n) {
      return String.format("%1$-" + n + "s", s);  
   }
}



//Class Table that is passed a name, list of arguments and list of corresponding types, both lists must be equal length
//as well as optional primary key and list of foreign keys in a foreign array
//Foreign keys are in the form of a subarray consisting of:
//0) local attribute that is a foreign key
//1) table foreign key references
//2) referenced attribute
class Table {
      String name;
      String[] arglist;
      String[] typelist;
      String[][] foreign;
      String pkey;
      int linecount = 0;
   public Table(){
   }   
   public Table(String tablename, String ARGS[], String TYPES[], String FOREIGNK[][], String KEY){
      name = tablename;
      arglist = ARGS;
      typelist = TYPES;
      foreign = FOREIGNK;
      pkey = KEY;
   }   
   public String initializeTable(){
      String output = "CREATE TABLE IF NOT EXISTS " + name + " (" + arglist[0] + " " + typelist[0];      
      for (int i=1;i<arglist.length;i++){
         output += ", " + arglist[i] + " " + typelist[i];
      }      
      if (pkey.length() != 0){
         output += ", PRIMARY KEY(" + pkey + ")";
      }      
      for (int i=0;i<foreign.length;i++){
         output += ", FOREIGN KEY(" + foreign[i][0] + ") REFERENCES " + foreign[i][1] + "(" + foreign[i][2] + ")";
      }      
      output += ")";      
      return output;
   }
   
   public String insertValues(String[] vals){      
      String output = "INSERT INTO " + name + " VALUES (" + vals[0];           
      for (int i=1;i<arglist.length;i++){
         if (i < vals.length){
            if (vals[i].equals("true")){
               output += ", 1";            
            } else if (vals[i].equals("false")) {
               output += ", 0";
            } else if (vals[i].length() > 0) {
               output += ", " + vals[i];            
            } else {
            output += ", " + "NULL";
            }
         } else {
            output += ", " + "NULL";
         }
      }     
      output += ");";     
      return output;
   }
}