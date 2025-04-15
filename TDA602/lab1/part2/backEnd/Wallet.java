package backEnd;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

public class Wallet {
    /**
     * The File of the wallet file
     */  
    private final File file;

    /**
     * Creates a Wallet object
     *
     * A Wallet object interfaces with the wallet File
     */
    public Wallet () throws Exception {
	    this.file = new File("backEnd/wallet.txt");
    }

    /**
     * Gets the wallet balance. 
     *
     * @return                   The content of the wallet file as an integer
     */
    public int getBalance() throws Exception {
        for (int i = 0; i < 10; i++) {
            try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                raf.seek(0);
                return Integer.parseInt(raf.readLine());
            } catch (IOException e) {
                System.out.println("File is locked, waiting to read...");
                wait(5000);
            }
        }
        throw new Exception("Could not access the file.");
    }

    private int getBalance(RandomAccessFile raf) throws Exception {
        raf.seek(0);
        return Integer.parseInt(raf.readLine());
    }

    /**
     * Sets a new balance in the wallet
     *
     * @param  newBalance          new balance to write in the wallet
     */
    private void setBalance(int newBalance, RandomAccessFile raf) throws Exception {
        raf.setLength(0);
        String str = Integer.valueOf(newBalance).toString()+'\n';
        raf.writeBytes(str);
    }

    public boolean safeWithdraw(int valueToWithdraw) throws Exception {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw");
            FileChannel channel = raf.getChannel();
            FileLock lock = channel.lock()) {
                
                int newBalance = getBalance(raf) - valueToWithdraw;

                // Sleep for 5 s to simulate data race
                wait(5000);

                // - check if the amount of credits is enough
                if (newBalance < 0) {
                    // if not stop the execution.
                    return false;
                } else {
                    // - otherwise, withdraw the price of the product from the wallet.
                    setBalance(newBalance, raf);
                    return true;
                }
            }
    }

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
}
