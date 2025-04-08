package backEnd;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Lock;

public class Wallet {
    /**
     * The RandomAccessFile of the wallet file
     */  
    private RandomAccessFile file;

    private static void wait(int ms) {
        try {
            Thread.sleep(ms);
        } catch (Exception e) {
        }
    }

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
	this.file.seek(0);
	return Integer.parseInt(this.file.readLine());
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
    public void close() throws Exception {
	this.file.close();
    }

    private final Lock lockObj = new ReentrantLock();

    public boolean safeWithdraw(int valueToWithdraw) throws Exception {
        lockObj.lock();
        try {
            int balance = this.getBalance();
            wait(5000);
            int newBalance = balance - valueToWithdraw;
            if (balance >= valueToWithdraw){
                this.setBalance(newBalance);
                return true;
            }
            return false;
        }finally{
            lockObj.unlock();
        }
    }
}
