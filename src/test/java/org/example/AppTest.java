package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.example.dao.AccountDao;
import org.example.db.DbManager;
import org.example.entity.Account;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

import static java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;
import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;

@Slf4j
public class AppTest {
    private static final Logger logger = LoggerFactory.getLogger(AppTest.class.getName());
    private static final DbManager dbManager = DbManager.getInstance();
    private static final AccountDao accountDao = new AccountDao();
    private static CountDownLatch countDownLatch;
    private double amount;
    
    @Before
    public void beforeTest() throws SQLException, FileNotFoundException {
        executeSqlScript();
        countDownLatch = new CountDownLatch(2);
        amount = 0;
    }

    @Test
    public void dirtyReadTest() throws InterruptedException {
        logger.info("Dirty read test start.....");
        withdrawTest(TRANSACTION_READ_UNCOMMITTED);
        Assert.assertEquals(-200, amount, 0);
        logger.info("Dirty read test end.");
    }

    @Test
    public void repeatableReadTest() throws InterruptedException {
        logger.info("Repeatable read test start.....");
        withdrawTest(TRANSACTION_REPEATABLE_READ);
        Assert.assertEquals(-1200, amount, 0);
        logger.info("Repeatable read test end.....");
    }

    private void withdrawTest(int isolationLevel) throws InterruptedException {
        Account testAccount = accountDao.findById(1);
        logger.info(String.format("processing: %s", testAccount));
        Thread thread1 = new Thread(() -> {
            try {
                logger.info(String.format("%s: start...", Thread.currentThread().getName()));
                accountDao.withdraw(testAccount, 300, 1000, isolationLevel, 3);
                countDownLatch.countDown();
                logger.info(String.format("%s: done.", Thread.currentThread().getName()));
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        Thread thread2 = new Thread(() -> {
            try {
                logger.info(String.format("%s: start...", Thread.currentThread().getName()));
                accountDao.withdraw(testAccount, 1000, 200, isolationLevel, 3);
                countDownLatch.countDown();
                logger.info(String.format("%s: done.", Thread.currentThread().getName()));
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        thread1.start();
        thread2.start();
        countDownLatch.await();
        amount = accountDao.findById(1).getBalance();
        Account resultAccount = accountDao.findById(1);
        logger.info(String.format("Result: %s", resultAccount));
    }

    private static void executeSqlScript() throws SQLException, FileNotFoundException {
        Connection conn = dbManager.getConnection();
        Reader reader = new BufferedReader(new FileReader("src/test/resources/Account.sql"));
        ScriptRunner sr = new ScriptRunner(conn);
        sr.setAutoCommit(true);
        sr.setStopOnError(true);
        sr.runScript(reader);
    }
}
