package edu.uw.cs;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.security.*;
import java.security.spec.*;
import javax.crypto.*;
import javax.crypto.spec.*;
/**
 * Runs queries against a back-end database
 */
public class Query {
  // DB Connection
  private Connection conn;

  // Password hashing parameter constants
  private static final int HASH_STRENGTH = 65536;
  private static final int KEY_LENGTH = 128;

  // My Fields
  private String username;
  private List<Itinerary> itineraries;

  // Canned queries
  private static final String CHECK_FLIGHT_CAPACITY = "SELECT capacity FROM Flights WHERE fid = ?";
  private PreparedStatement checkFlightCapacityStatement;
  // TODO: YOUR CODE HERE
  private static final String CLEAR_TABLES = "DELETE FROM Reservations; DELETE FROM Users; DELETE FROM ReservationID;";
  private PreparedStatement clearTablesStmt;
  private static final String CREATE_USER = "INSERT INTO Users VALUES (?,?,?,?)";
  private PreparedStatement createUserStmt;

  private static final String GET_HASHSALT = "SELECT salt, hash FROM Users WHERE username = ?";
  private PreparedStatement getHashSaltStmt;

  private static final String GET_DIRECT = "SELECT top (?) fid, day_of_month, carrier_id, flight_num, origin_city, dest_city, actual_time, capacity, price " +
                                           "FROM flights where origin_city = ? AND dest_city = ? AND day_of_month = ? " +
                                           "ORDER BY actual_time ASC, fid ASC";
  private PreparedStatement getDirectStmt;

  private static final String GET_INDIRECT = "SELECT TOP (?) f1.fid AS afid, f1.day_of_month AS aday_of_month, f1.carrier_id AS acarrier_id, f1.flight_num AS aflight_num, f1.origin_city AS aorigin_city, f1.dest_city AS adest_city, f1.actual_time AS aactual_time, f1.capacity AS acapacity, f1.price AS aprice, " +
                                             "f2.fid AS bfid, f2.day_of_month AS bday_of_month, f2.carrier_id AS bcarrier_id, f2.flight_num AS bflight_num, f2.origin_city AS borigin_city, f2.dest_city AS bdest_city, f2.actual_time AS bactual_time, f2.capacity AS bcapacity, f2.price AS bprice " +
                                             "FROM Flights AS f1, Flights AS f2 " +
                                             "WHERE f1.origin_city = ? AND f1.dest_city = f2.origin_city AND f2.dest_city = ? AND f1.day_of_month = ? AND f1.day_of_month = f2.day_of_month  AND f1.canceled = 0 AND f2.canceled = 0 " +
                                             "ORDER BY f1.actual_time + f2.actual_time ASC, f1.fid ASC, f2.fid ASC";
  private PreparedStatement getIndirectStmt;

  private static final String CHECK_DAY_RESERVED = "SELECT count(*) AS count FROM Reservations WHERE username = ? AND day = ?";
  private PreparedStatement checkDayReservedStmt;

  private static final String GET_NEXT_RID = "SELECT rid FROM ReservationID";
  private PreparedStatement getNextRidStmt;

  private static final String PUT_NEXT_RID = "INSERT INTO ReservationID VALUES (?)";
  private PreparedStatement insertRidStmt;

  private static final String BOOK_RESERVATION = "INSERT INTO Reservations " +
                                                 "VALUES (?, ?, ?, ?, ?, ?, ?)";
  private PreparedStatement bookReservationStmt;

  private static final String RESET_ID = "INSERT INTO ReservationID VALUES(1)";
  private PreparedStatement resetIDStmt;

  private static final String GET_RESERVATION = "SELECT price FROM Reservations WHERE rid = ? AND username = ? AND paid = 0";
  private PreparedStatement getReservationStmt;

  private static final String GET_BALANCE = "SELECT balance FROM Users WHERE username = ?";
  private PreparedStatement getBalanceStmt;

  private static final String UPDATE_RESERVATION = "UPDATE Reservations SET paid = 1 WHERE username = ? AND rid = ?";
  private PreparedStatement updatePaidReservationStmt;

  private static final String UPDATE_BALANCE = "UPDATE Users SET balance = ? WHERE username = ?";
  private PreparedStatement updateBalanceStmt;

  private static final String FIND_RESERVATIONS_ON_USERNAME = "SELECT rid, fid1, fid2, day, price, paid FROM Reservations WHERE username =?";
  private PreparedStatement findReservationsOnUsernameStmt;
  
  private static final String GET_FLIGHT_ON_FID = "SELECT fid, day_of_month, carrier_id, flight_num, origin_city, dest_city, actual_time, capacity, price " +
                                           "FROM flights where fid = ?";
  private PreparedStatement findFlightStmt;            
  
  private static final String FIND_RESERVATION_TO_CANCEL = "SELECT price, paid FROM Reservations WHERE username =? AND rid =?";
  private PreparedStatement findReservationToCancelStmt;    
  
  private static final String DELETE_RESERVATION = "DELETE FROM Reservations WHERE rid=?;";
  private PreparedStatement deletReservationStmt;
  
  private static final String CHECK_USERNAME_EXISTS = "SELECT count(*) as count from Users where username = ?";
  private PreparedStatement checkUsernameStmt;
                      
  /**
   * Establishes a new application-to-database connection. Uses the
   * dbconn.properties configuration settings
   *
   * @throws IOException
   * @throws SQLException
   */
  public void openConnection() throws IOException, SQLException {
    // Connect to the database with the provided connection configuration
    Properties configProps = new Properties();
    configProps.load(new FileInputStream("dbconn.properties"));
    String serverURL = configProps.getProperty("hw1.server_url");
    String dbName = configProps.getProperty("hw1.database_name");
    String adminName = configProps.getProperty("hw1.username");
    String password = configProps.getProperty("hw1.password");
    String connectionUrl = String.format("jdbc:sqlserver://%s:1433;databaseName=%s;user=%s;password=%s", serverURL,
        dbName, adminName, password);
    conn = DriverManager.getConnection(connectionUrl);

    // By default, automatically commit after each statement
    conn.setAutoCommit(false);

    // By default, set the transaction isolation level to serializable
    conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    }

  /**
   * Closes the application-to-database connection
   */
  public void closeConnection() throws SQLException {
    conn.close();
  }

  /**
   * Clear the data in any custom tables created.
   *
   * WARNING! Do not drop any tables and do not clear the flights table.
   */
  public void clearTables() {
    try {
      // TODO: YOUR CODE HERE
      clearTablesStmt.executeUpdate();
      resetIDStmt.executeUpdate();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /*
   * prepare all the SQL statements in this method.
   */
  public void prepareStatements() throws SQLException {
    checkFlightCapacityStatement = conn.prepareStatement(CHECK_FLIGHT_CAPACITY);
    // TODO: YOUR CODE HERE
    clearTablesStmt = conn.prepareStatement(CLEAR_TABLES);
    createUserStmt = conn.prepareStatement(CREATE_USER);
    checkUsernameStmt = conn.prepareStatement(CHECK_USERNAME_EXISTS);
    getHashSaltStmt = conn.prepareStatement(GET_HASHSALT);
    getDirectStmt = conn.prepareStatement(GET_DIRECT);
    getIndirectStmt = conn.prepareStatement(GET_INDIRECT);
    getNextRidStmt = conn.prepareStatement(GET_NEXT_RID);
    insertRidStmt = conn.prepareStatement(PUT_NEXT_RID);
    bookReservationStmt = conn.prepareStatement(BOOK_RESERVATION);
    resetIDStmt = conn.prepareStatement(RESET_ID);
    getReservationStmt = conn.prepareStatement(GET_RESERVATION);
    getBalanceStmt = conn.prepareStatement(GET_BALANCE);
    updatePaidReservationStmt = conn.prepareStatement(UPDATE_RESERVATION);
    updateBalanceStmt = conn.prepareStatement(UPDATE_BALANCE);
    findReservationsOnUsernameStmt = conn.prepareStatement(FIND_RESERVATIONS_ON_USERNAME);
    findFlightStmt = conn.prepareStatement(GET_FLIGHT_ON_FID);
    findReservationToCancelStmt = conn.prepareStatement(FIND_RESERVATION_TO_CANCEL);
    deletReservationStmt = conn.prepareStatement(DELETE_RESERVATION);
    checkDayReservedStmt = conn.prepareStatement(CHECK_DAY_RESERVED);
  }

  /**
   * Takes a user's username and password and attempts to log the user in.
   *
   * @param username user's username
   * @param password user's password
   *
   * @return If someone has already logged in, then return "User already logged
   *         in\n" For all other errors, return "Login failed\n". Otherwise,
   *         return "Logged in as [username]\n".
   */
  public String transaction_login(String username, String password) {
    // TODO: YOUR CODE HERE
    //System.out.println("here");
    if (this.username != null) {
        return "User already logged in\n";
    } else {
      try {
        getHashSaltStmt.clearParameters();
        getHashSaltStmt.setString(1, username);
        ResultSet results = getHashSaltStmt.executeQuery();
        results.next();
        byte[] salt = results.getBytes("salt");
        byte[] retrievedHash = results.getBytes("hash");
        results.close();

        //System.out.println("Salt is: " + Arrays.toString(salt));
        //System.out.println("Retrieved Hash is: " + Arrays.toString(retrievedHash));

        // Specify the hash parameters
        KeySpec spec = new PBEKeySpec(password.toCharArray(), salt, HASH_STRENGTH, KEY_LENGTH);

        // Generate the hash
        SecretKeyFactory factory = null;
        byte[] hash = null;
        try {
          factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
          hash = factory.generateSecret(spec).getEncoded();
        } catch (NoSuchAlgorithmException | InvalidKeySpecException ex) {
          throw new IllegalStateException();
        }
        //System.out.println("hash is: " + Arrays.toString(hash));
        if (Arrays.equals(retrievedHash, hash)) {
          this.username = username;
          conn.commit();
          return "Logged in as " + username + "\n";
        }
      } catch (SQLException ex) {
        
        try {
            conn.rollback();
            return "Login failed\n";
        } catch (SQLException e) {
            e.printStackTrace();
            return "Login failed\n";
        }
        
      }
    }
    return "Login failed\n";
  }

  /**
   * Implement the create user function.
   *
   * @param username   new user's username. User names are unique the system.
   * @param password   new user's password.
   * @param initAmount initial amount to deposit into the user's account, should
   *                   be >= 0 (failure otherwise).
   *
   * @return either "Created user {@code username}\n" or "Failed to create user\n"
   *         if failed.
   */
  public String transaction_createCustomer(String username, String password, int initAmount) {
    // TODO: YOUR CODE HERE
    if (usernameExists(username) || initAmount < 0)
      return "Failed to create user\n";
    else {
      try {
        createUserStmt.clearParameters();
        createUserStmt.setString(1, username);
        // Generate a random cryptographic salt
        SecureRandom random = new SecureRandom();
        byte[] salt = new byte[16];
        random.nextBytes(salt);
        createUserStmt.setBytes(3, salt);

        // Specify the hash parameters
        KeySpec spec = new PBEKeySpec(password.toCharArray(), salt, HASH_STRENGTH, KEY_LENGTH);

        // Generate the hash
        SecretKeyFactory factory = null;
        byte[] hash = null;
        try {
          factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
          hash = factory.generateSecret(spec).getEncoded();
        } catch (NoSuchAlgorithmException | InvalidKeySpecException ex) {
          throw new IllegalStateException();
        }
        createUserStmt.setBytes(2, hash);
        createUserStmt.setInt(4, initAmount);
        createUserStmt.executeUpdate();
        conn.commit();
      } catch (SQLException ex) {
        try {
           conn.rollback();
           return "Failed to create user\n";
        } catch (SQLException e) {
           e.printStackTrace();
           return "Failed to create user\n";
        }
        
      }
      return "Created user " + username + "\n";
    }
  }


  /**
   * Implement the search function.
   *
   * Searches for flights from the given origin city to the given destination
   * city, on the given day of the month. If {@code directFlight} is true, it only
   * searches for direct flights, otherwise is searches for direct flights and
   * flights with two "hops." Only searches for up to the number of itineraries
   * given by {@code numberOfItineraries}.
   *
   * The results are sorted based on total flight time.
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight        if true, then only search for direct flights,
   *                            otherwise include indirect flights as well
   * @param dayOfMonth
   * @param numberOfItineraries number of itineraries to return
   *
   * @return If no itineraries were found, return "No flights match your
   *         selection\n". If an error occurs, then return "Failed to search\n".
   *
   *         Otherwise, the sorted itineraries printed in the following format:
   *
   *         Itinerary [itinerary number]: [number of flights] flight(s), [total
   *         flight time] minutes\n [first flight in itinerary]\n ... [last flight
   *         in itinerary]\n
   *
   *         Each flight should be printed using the same format as in the
   *         {@code Flight} class. Itinerary numbers in each search should always
   *         start from 0 and increase by 1.
   *
   * @see Flight#toString()
   */
  public String transaction_search(String originCity, String destinationCity, boolean directFlight, int dayOfMonth,
      int numberOfItineraries) {
    // WARNING the below code is unsafe and only handles searches for direct flights
    // You can use the below code as a starting reference point or you can get rid
    // of it all and replace it with your own implementation.
    //
    // TODO: YOUR CODE HERE
    itineraries = new ArrayList<Itinerary>();
    StringBuffer sb = new StringBuffer();

    try {

      getDirectStmt.clearParameters();
      getDirectStmt.setInt(1, numberOfItineraries);
      getDirectStmt.setString(2, originCity);
      getDirectStmt.setString(3, destinationCity);
      getDirectStmt.setInt(4, dayOfMonth);

      ResultSet oneHopResults = getDirectStmt.executeQuery();

      int index = 0; // itinerary id
      if (!oneHopResults.next()) {
         return "No flights match your selection\n";
      }
      do {
        int result_fid = oneHopResults.getInt("fid");
        int result_dayOfMonth = oneHopResults.getInt("day_of_month");
        String result_carrierId = oneHopResults.getString("carrier_id");
        String result_flightNum = oneHopResults.getString("flight_num");
        String result_originCity = oneHopResults.getString("origin_city");
        String result_destCity = oneHopResults.getString("dest_city");
        int result_time = oneHopResults.getInt("actual_time");
        int result_capacity = oneHopResults.getInt("capacity");
        int result_price = oneHopResults.getInt("price");

        itineraries.add(new Itinerary(new Flight(result_fid, result_dayOfMonth, result_carrierId, result_flightNum, result_originCity, result_destCity, result_time, result_capacity, result_price)));
        index++;
      } while (oneHopResults.next());
      oneHopResults.close();

      if (!directFlight) {
        int itinerariesRemaining = numberOfItineraries - index;
        if (itinerariesRemaining > 0) {
          getIndirectStmt.clearParameters();
          getIndirectStmt.setInt(1, itinerariesRemaining);
          getIndirectStmt.setString(2, originCity);
          getIndirectStmt.setString(3, destinationCity);
          getIndirectStmt.setInt(4, dayOfMonth);

          ResultSet twoHopResults = getIndirectStmt.executeQuery();

          while (twoHopResults.next()) {
            int afid = twoHopResults.getInt("afid");
            int adayOfMonth = twoHopResults.getInt("aday_of_month");
            String acarrierId = twoHopResults.getString("acarrier_id");
            String aflightNum = twoHopResults.getString("aflight_num");
            String aoriginCity = twoHopResults.getString("aorigin_city");
            String adestCity = twoHopResults.getString("adest_city");
            int atime = twoHopResults.getInt("aactual_time");
            int acapacity = twoHopResults.getInt("acapacity");
            int aprice = twoHopResults.getInt("aprice");

            int bfid = twoHopResults.getInt("bfid");
            int bdayOfMonth = twoHopResults.getInt("bday_of_month");
            String bcarrierId = twoHopResults.getString("bcarrier_id");
            String bflightNum = twoHopResults.getString("bflight_num");
            String boriginCity = twoHopResults.getString("borigin_city");
            String bdestCity = twoHopResults.getString("bdest_city");
            int btime = twoHopResults.getInt("bactual_time");
            int bcapacity = twoHopResults.getInt("bcapacity");
            int bprice = twoHopResults.getInt("bprice");
            
            Flight firstF = new Flight(afid, adayOfMonth, acarrierId, aflightNum, aoriginCity, adestCity, atime, acapacity, aprice);

            Flight secondF = new Flight(bfid, bdayOfMonth, bcarrierId, bflightNum, boriginCity, bdestCity, btime, bcapacity, bprice);

            itineraries.add(new Itinerary(firstF, secondF));
          }
          twoHopResults.close();
        }
      }
      Collections.sort(itineraries);
      for (int i = 0; i < itineraries.size(); i++) {
        Itinerary temp = itineraries.get(i);
        sb.append("Itinerary " + i + ": " + temp.count + " flight(s), " + temp.totalTime + " minutes" + "\n");
        sb.append(temp.toString());
      }
    } catch (SQLException ex) {
      try {
         // recursive retry
         return transaction_search(originCity, destinationCity, directFlight, dayOfMonth, numberOfItineraries);
         
      } catch (SQLException e) {
         e.printStackTrace();
         return "Failed to search\n";
      }
      
    }
    return sb.toString();
  }

  /**
   * Implements the book itinerary function.
   *
   * @param itineraryId ID of the itinerary to book. This must be one that is
   *                    returned by search in the current session.
   *
   * @return If the user is not logged in, then return "Cannot book reservations,
   *         not logged in\n". If try to book an itinerary with invalid ID, then
   *         return "No such itinerary {@code itineraryId}\n". If the user already
   *         has a reservation on the same day as the one that they are trying to
   *         book now, then return "You cannot book two flights in the same
   *         day\n". For all other errors, return "Booking failed\n".
   *
   *         And if booking succeeded, return "Booked flight(s), reservation ID:
   *         [reservationId]\n" where reservationId is a unique number in the
   *         reservation system that starts from 1 and increments by 1 each time a
   *         successful reservation is made by any user in the system.
   */
  public String transaction_book(int itineraryId) {
    // TODO: YOUR CODE HERE
    if (this.username == null) {
      return "Cannot book reservations, not logged in\n";
    } else if (itineraries == null) {
      //return "Booking failed\n";
      return "No such itinerary " + itineraryId + "\n";
    } else if (itineraryId < 0 || itineraryId >= itineraries.size()) { 
      return "No such itinerary " + itineraryId + "\n";
    } else {
      try {
        Itinerary targetBook = itineraries.get(itineraryId);
        
        checkDayReservedStmt.clearParameters();
        checkDayReservedStmt.setString(1, this.username);
        checkDayReservedStmt.setInt(2, targetBook.f1.dayOfMonth);
        // check if user already has a reservation on the same day
        ResultSet results = checkDayReservedStmt.executeQuery();
        results.next();
        int count = results.getInt("count");
        results.close();
        if (count != 0) {
          return "You cannot book two flights in the same day\n";
        }
        // Get the next reservation ID
        results = getNextRidStmt.executeQuery();
        results.next();
        
        int reservationID = results.getInt("rid");
        results.close();
        int price = targetBook.f1.price;
        bookReservationStmt.clearParameters();
        bookReservationStmt.setInt(1, reservationID);
        bookReservationStmt.setInt(2, 0);
        bookReservationStmt.setString(3, this.username);
        bookReservationStmt.setInt(4, targetBook.f1.fid);
        if (targetBook.f2 != null) {
          bookReservationStmt.setInt(5, targetBook.f2.fid);
          price += targetBook.f2.price;
        } else {
          bookReservationStmt.setNull(5, java.sql.Types.INTEGER);
        }
        bookReservationStmt.setInt(6, price);
        bookReservationStmt.setInt(7, targetBook.f1.dayOfMonth);
        
        
        bookReservationStmt.executeUpdate();

        // update the new reservation ID
        insertRidStmt.clearParameters();
        insertRidStmt.setInt(1, reservationID + 1);
        insertRidStmt.executeUpdate();
        conn.commit();
        return "Booked flight(s), reservation ID: " + reservationID + "\n";
      } catch (SQLException ex) {
        try {
         conn.rollback();
         return "Booking failed\n";
        } catch (SQLException e) {
         e.printStackTrace();
         return "Booking failed\n";
        }
        
      }
    }
  }

  /**
   * Implements the pay function.
   *
   * @param reservationId the reservation to pay for.
   *
   * @return If no user has logged in, then return "Cannot pay, not logged in\n"
   *         If the reservation is not found / not under the logged in user's
   *         name, then return "Cannot find unpaid reservation [reservationId]
   *         under user: [username]\n" If the user does not have enough money in
   *         their account, then return "User has only [balance] in account but
   *         itinerary costs [cost]\n" For all other errors, return "Failed to pay
   *         for reservation [reservationId]\n"
   *
   *         If successful, return "Paid reservation: [reservationId] remaining
   *         balance: [balance]\n" where [balance] is the remaining balance in the
   *         user's account.
   */
  public String transaction_pay(int reservationId) {
    // TODO: YOUR CODE HERE
    if (username == null) {
      return "Cannot pay, not logged in\n";
    } else {
      try {
      
        getReservationStmt.clearParameters();
        getReservationStmt.setString(2, this.username);
        getReservationStmt.setInt(1, reservationId);
        
        ResultSet results = getReservationStmt.executeQuery();
        if (!results.next()) {
          return "Cannot find unpaid reservation " + reservationId + " under user: " + this.username + "\n";
        }
        int price = results.getInt("price");
        //System.out.println("found ticket price: "+ price);
        results.close();
        getBalanceStmt.clearParameters();
        getBalanceStmt.setString(1, this.username);
        ResultSet checkBalance = getBalanceStmt.executeQuery();
        checkBalance.next();
        int balance = checkBalance.getInt("balance");
        checkBalance.close();
        
        if (price > balance) {
          return "User has only " + balance + " in account but itinerary costs " + price + "\n";
        }
        
        updatePaidReservationStmt.clearParameters();
        updatePaidReservationStmt.setString(1, this.username);
        updatePaidReservationStmt.setInt(2, reservationId);
        updatePaidReservationStmt.executeUpdate();
        updateBalanceStmt.clearParameters();
        updateBalanceStmt.setInt(1, balance - price);
        updateBalanceStmt.setString(2, this.username);
        updateBalanceStmt.executeUpdate();
        balance = balance - price;
        conn.commit();
        return "Paid reservation: " + reservationId + " remaining balance: " + balance + "\n";
      } catch (SQLException ex) {
         try {
            conn.rollback();
            return "Failed to pay for reservation " + reservationId + "\n";
         } catch (SQLException e) {
            e.printStackTrace();
            return "Failed to pay for reservation " + reservationId + "\n";
         }
        
      }
    }

  }

  /**
   * Implements the reservations function.
   *
   * @return If no user has logged in, then return "Cannot view reservations, not
   *         logged in\n" If the user has no reservations, then return "No
   *         reservations found\n" For all other errors, return "Failed to
   *         retrieve reservations\n"
   *
   *         Otherwise return the reservations in the following format:
   *
   *         Reservation [reservation ID] paid: [true or false]:\n" [flight 1
   *         under the reservation] [flight 2 under the reservation] Reservation
   *         [reservation ID] paid: [true or false]:\n" [flight 1 under the
   *         reservation] [flight 2 under the reservation] ...
   *
   *         Each flight should be printed using the same format as in the
   *         {@code Flight} class.
   *
   * @see Flight#toString()
   */
  public String transaction_reservations() {
    StringBuffer sb = new StringBuffer();
    if (username == null) {
      return "Cannot view reservations, not logged in\n";
    } else {
      try {
         findReservationsOnUsernameStmt.clearParameters();
         findReservationsOnUsernameStmt.setString(1, this.username);
         ResultSet results = findReservationsOnUsernameStmt.executeQuery();
         if (!results.next()) {
            return "No reservations found\n";
         }
         //results.beforeFirst(); 
         
         do {
            
            int rid = results.getInt("rid");
            //System.out.println("found rid is: " + rid);
            int fid1 = results.getInt("fid1");
            //System.out.println("fid1 is: " + fid1);
            int fid2 = results.getInt("fid2");
            //System.out.println("fid2 is: " + fid2);
            //int day = results.getInt("day");
            int price = results.getInt("price");
            int paid = results.getInt("paid");
            String resPaid = (paid == 1 ) ? "true"  : "false";
            
            //get flights info from fid
            //fid, day_of_month, carrier_id, flight_num, origin_city, dest_city, actual_time, capacity, price
            findFlightStmt.clearParameters();
            findFlightStmt.setInt(1, fid1);
            ResultSet firstResult = findFlightStmt.executeQuery();
            firstResult.next();
            
            //collect info for 1st flight
            int output_dayOfMonth1 = firstResult.getInt("day_of_month");
            String output_carrierId1 = firstResult.getString("carrier_id");
            String output_flightNum1 = firstResult.getString("flight_num");
            String output_originCity1 = firstResult.getString("origin_city");
            String output_destCity1 = firstResult.getString("dest_city");
            int output_time1 = firstResult.getInt("actual_time");
            int output_capacity1 = firstResult.getInt("capacity");
            int output_price1 = firstResult.getInt("price");
            //System.out.println("found flight flightNum1: " + output_flightNum1);
            Itinerary reservation;
            //one flight reservation
            if (fid2 == 0) {
               reservation = new Itinerary(new Flight(fid1, output_dayOfMonth1,
                                                         output_carrierId1,output_flightNum1,output_originCity1,
                                                         output_destCity1,output_time1,output_capacity1,output_price1)); 
            } else {//two flight reservation
            
               //collect info for 2nd flight
               findFlightStmt.clearParameters();
               findFlightStmt.setInt(1, fid2);
               ResultSet secondResult = findFlightStmt.executeQuery();
               secondResult.next();
               int output_dayOfMonth2 = secondResult.getInt("day_of_month");
               String output_carrierId2 = secondResult.getString("carrier_id");
               String output_flightNum2 = secondResult.getString("flight_num");
               String output_originCity2 = secondResult.getString("origin_city");
               String output_destCity2 = secondResult.getString("dest_city");
               int output_time2 = secondResult.getInt("actual_time");
               int output_capacity2 = secondResult.getInt("capacity");
               int output_price2 = secondResult.getInt("price");
               reservation = new Itinerary(new Flight(fid1, output_dayOfMonth1,
                                                         output_carrierId1,output_flightNum1,output_originCity1,
                                                         output_destCity1,output_time1,output_capacity1,output_price1), 
                                                     new Flight(fid2, output_dayOfMonth2,
                                                         output_carrierId2,output_flightNum2,output_originCity2,
                                                         output_destCity2,output_time2,output_capacity2,output_price2));
                                                         
            }
            
            sb.append("Reservation " + rid+ " paid: " + resPaid + ":\n" + reservation.toString());
            
            
         } while (results.next()); 
         conn.commit();
         return sb.toString();    
      } catch (SQLException ex) {
         try {
            // recursive retry
            return transaction_reservations();
         } catch (SQLException e) {
            e.printStackTrace();
            return "Failed to retrieve reservations\n";
         }
        
      }
      
    }
  }

  /**
   * Implements the cancel operation.
   *
   * @param reservationId the reservation ID to cancel
   *
   * @return If no user has logged in, then return "Cannot cancel reservations,
   *         not logged in\n" For all other errors, return "Failed to cancel
   *         reservation [reservationId]\n"
   *
   *         If successful, return "Canceled reservation [reservationId]\n"
   *
   *         Even though a reservation has been canceled, its ID should not be
   *         reused by the system.
   */
  public String transaction_cancel(int reservationId) {
    // TODO: YOUR CODE HERE
    if (username == null) {
    return "Cannot cancel reservations, not logged in\n";
    }
    
    try {
      // find reservation to cancel
      findReservationToCancelStmt.clearParameters();
      findReservationToCancelStmt.setString(1, this.username);
      findReservationToCancelStmt.setInt(2, reservationId);
      ResultSet result = findReservationToCancelStmt.executeQuery();
      if (!result.next()) {
         return "Failed to cancel reservation " + reservationId + "\n";
      }
      
      int price = result.getInt("price");
      int paid = result.getInt("paid");
      
      //delete reservation
      deletReservationStmt.clearParameters();
      deletReservationStmt.setInt(1,reservationId);
      deletReservationStmt.executeUpdate();
      
      //refund if paid
      if (paid == 1) {
         // get balance
         getBalanceStmt.clearParameters();
         getBalanceStmt.setString(1, this.username);
         ResultSet checkBalance = getBalanceStmt.executeQuery();
         checkBalance.next();
         int balance = checkBalance.getInt("balance");
         checkBalance.close();
         
         // update balance
         updateBalanceStmt.clearParameters();
         updateBalanceStmt.setInt(1, balance + price);
         updateBalanceStmt.setString(2, this.username);
         updateBalanceStmt.executeUpdate();

         
      }
    conn.commit();
    return "Canceled reservation "+ reservationId + "\n";
    } catch (SQLException ex) {
      try {
         conn.rollback();
         return "Failed to cancel reservation " + reservationId + "\n";
      } catch (SQLException e) {
         e.printStackTrace();
         return "Failed to cancel reservation " + reservationId + "\n";
      }
    }
    
  }

  /**
   * Example utility function that uses prepared statements
   */
  private int checkFlightCapacity(int fid) throws SQLException {
    checkFlightCapacityStatement.clearParameters();
    checkFlightCapacityStatement.setInt(1, fid);
    ResultSet results = checkFlightCapacityStatement.executeQuery();
    results.next();
    int capacity = results.getInt("capacity");
    results.close();

    return capacity;
  }
   
  private boolean usernameExists(String username) {
    try {
      checkUsernameStmt.clearParameters();
      checkUsernameStmt.setString(1, username);
      ResultSet results = checkUsernameStmt.executeQuery();
      results.next();
      int count = results.getInt("count");
      //System.out.println("count is :" + count);
      results.close();
      return count != 0;
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return false;
  }
  
  /**
   * A class to store flight information.
   */
  class Flight {
    public int fid;
    public int dayOfMonth;
    public String carrierId;
    public String flightNum;
    public String originCity;
    public String destCity;
    public int time;
    public int capacity;
    public int price;

    public Flight(int fid, int dayOfMonth, String carrierId, String flightNum, String originCity, String destCity, int time, int capacity, int price) {
      this.fid = fid;
      this.dayOfMonth = dayOfMonth;
      this.carrierId = carrierId;
      this.flightNum = flightNum;
      this.originCity = originCity;
      this.destCity = destCity;
      this.time = time;
      this.capacity = capacity;
      this.price = price;
    }

    @Override
    public String toString() {
      return "ID: " + fid + " Day: " + dayOfMonth + " Carrier: " + carrierId + " Number: " + flightNum + " Origin: "
          + originCity + " Dest: " + destCity + " Duration: " + time + " Capacity: " + capacity + " Price: " + price;
    }
  }
  
  class Itinerary implements Comparable<Itinerary> {
    public Flight f1;
    public Flight f2;
    public int totalTime;
    public int count;
    
    public Itinerary(Flight f1, Flight f2) {
      this.f1 = f1;
      this.f2 = f2;
      this.totalTime = f1.time + f2.time;
      this.count = 2;
    }
    
    public Itinerary(Flight f1) {
      this.f1 = f1;
      this.totalTime = f1.time;
      this.count = 1;
    }
    
    @Override
    public String toString() {
      if (count == 1) {
         return this.f1.toString() + "\n";
      } else {
         return this.f1.toString() + "\n" + this.f2.toString() + "\n";
      }
    }
    


    @Override
    public int compareTo(Itinerary other) {
      int time = this.totalTime - other.totalTime;
      if (time != 0) {
         return time;
      } else {
           int fid1 = this.f1.fid - other.f1.fid;
           if (fid1 == 0 && this.f2 != null && other.f2 != null) {
             return this.f2.fid - other.f2.fid;
           }
           return fid1;
      }
      
    }
  }
  
  



}
