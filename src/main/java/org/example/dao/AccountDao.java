package org.example.dao;

import org.example.db.DbManager;
import org.example.entity.Account;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class AccountDao {
    private static final String EXCEPTION_MESSAGE = "SQL Exception: %s";

    private static final String SQL_FIND_ALL = "select * from account";

    private static final String SQL_FIND_BY_ID = "select * from account where id = ?";

    private static final String SQL_UPDATE = "update account set balance = ? where id = ?";


    DbManager dbManager = DbManager.getInstance();

    private static Logger logger = LoggerFactory.getLogger(AccountDao.class.getName());

    public List<Account> findAll() {
        List<Account> list = new ArrayList<>();

        try (Connection connection = dbManager.getConnection();
             Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery(SQL_FIND_ALL)) {

            while (rs.next()) {
                Account account = new Account(
                        rs.getInt(1), rs.getString(2), rs.getString(3),
                        rs.getDouble(4));
                list.add(account);
            }
        } catch (SQLException ex) {
            logger.info(String.format(EXCEPTION_MESSAGE, ex.getMessage()));
        }
        return list;
    }

    public Account findById(Integer id) {

        try (Connection connection = dbManager.getConnection();
             PreparedStatement pst = connection.prepareStatement(SQL_FIND_BY_ID)) {

            pst.setInt(1, id);
            ResultSet rs = pst.executeQuery();
            connection.commit();
            rs.next();
            return new Account(
                    rs.getInt(1), rs.getString(2), rs.getString(3),
                    rs.getDouble(4));
        } catch (SQLException ex) {
            logger.info(String.format(EXCEPTION_MESSAGE, ex.getMessage()));
        }
        return null;
    }

    public void withdraw(Account account, double amount, long sleepTime, int isolationLevel, int retries) throws InterruptedException {
        logger.info(String.format("%s: withdraw start... withdraw amount: %s", Thread.currentThread().getName(), amount));
        try (Connection connection = dbManager.getConnection()) {
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(isolationLevel);
            PreparedStatement pstmt = connection.prepareStatement(SQL_FIND_BY_ID);
            pstmt.setInt(1, account.getId());
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                double balance = rs.getDouble("balance");
                logger.info(String.format("%s: balance: %s", Thread.currentThread().getName(), balance));
                double newBalance = balance - amount;
                Thread.sleep(sleepTime);
                PreparedStatement pstmt2 = connection.prepareStatement(SQL_UPDATE);
                pstmt2.setDouble(1, newBalance);
                pstmt2.setInt(2, account.getId());
                pstmt2.executeUpdate();
                connection.commit();
                account.setBalance(newBalance);
                logger.info(String.format("%s: withdraw finish... balance: %s", Thread.currentThread().getName(), newBalance));
            }
        } catch (SQLException ex) {
            logger.info(String.format(EXCEPTION_MESSAGE, ex.getMessage()));
            if (retries > 0) {
                logger.info(String.format("retrying... retries left: %s ", retries));
                Thread.sleep(100);
                withdraw(account, amount, sleepTime, isolationLevel, retries - 1);
            }
        } catch (InterruptedException ex) {
            logger.info(String.format(EXCEPTION_MESSAGE, ex.getMessage()));
            Thread.currentThread().interrupt();
        }
    }
}
