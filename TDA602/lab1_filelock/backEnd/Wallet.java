package backEnd;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Lock;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

public class Wallet {
    /**
     * The RandomAccessFile of the wallet file
     */  
    private RandomAccessFile file;

    /**
     * Creates a Wallet object
     *
     * A Wallet object interfaces with the wallet RandomAccessFile
     */
    public Wallet () throws Exception {
	this.file = new RandomAccessFile(new File("backEnd/wallet.txt"), "rw");
    }

    /**
     * Gets the wallet balance. 
     *
     * @return                   The content of the wallet file as an integer
     */
    public int getBalance() throws IOException {
        FileChannel channel = file.getChannel();
        FileLock lock = channel.lock();
        try {
            this.file.seek(0);
            return Integer.parseInt(this.file.readLine());
        } finally {
            lock.release();
            channel.close();
        }
    }

    /**
     * Sets a new balance in the wallet
     *
     * @param  newBalance          new balance to write in the wallet
     */
    private void setBalance(int newBalance) throws Exception {
    this.file.setLength(0);
    String str = Integer.valueOf(newBalance).toString()+'\n'; 
    this.file.writeBytes(str); 
    }

    /**
     * Closes the RandomAccessFile in this.file
     */
    private void close() throws Exception {
	this.file.close();
    }

    /**
     * A wait function to simulate  concurrent execution
     * 
     * @param ms                 Amount of milliseconds to delay execution
     */
    private static void wait(int ms)
    {
        try
        {
            Thread.sleep(ms);
        }
        catch(InterruptedException ex)
        {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * A safe withdraw that avoids data races.
     * 
     * @param valueToWithdraw   amount to withdraw from the wallet
     * @return                  true if the withdraw was successful, false otherwise
     * @throws Exception
     */
    public boolean safeWithdraw(int valueToWithdraw) throws Exception {
        FileChannel channel = file.getChannel();
        FileLock lock = channel.lock();
        try {
            int currentBalance = getBalance();
            if (currentBalance >= valueToWithdraw) {
                wait(5000);
                setBalance(currentBalance - valueToWithdraw);
                return true;
            }
            return false;
        } finally {
            lock.release();
            channel.close();
        }
    }
}
