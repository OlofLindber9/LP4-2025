import backEnd.*;
import java.util.Scanner;

public class ShoppingCart {
    private static void print(Wallet wallet, Pocket pocket) throws Exception {
        System.out.println("Your current balance is: " + wallet.getBalance() + " credits.");
        System.out.println(Store.asString());
        System.out.println("Your current pocket is:\n" + pocket.getPocket());
    }

    private static String scan(Scanner scanner) throws Exception {
        System.out.print("What do you want to buy? (type quit to stop) ");
        return scanner.nextLine();
    }

    public static void main(String[] args) throws Exception {
        Wallet wallet = new Wallet();
        Pocket pocket = new Pocket();
        Scanner scanner = new Scanner(System.in);

        print(wallet, pocket);
        String product = scan(scanner);

        while(!product.equals("quit")) {
            // Simulate a data race by entering "simulateRace".
            if(product.equals("simulateRace")) {
                simulateRace(wallet, pocket);
            } else {
                // returns true if purchase went through
                boolean purchase = makePurchase(product, wallet, pocket);
                
                // if not stop the execution.
                if (!purchase) break;
            }
                // Just to print everything again...
                print(wallet, pocket);
                product = scan(scanner);
        }
    }

    private static boolean makePurchase(String product, Wallet wallet, Pocket pocket) throws Exception{
        int newBalance = wallet.getBalance() - Store.getProductPrice(product);

        // Delay execution after call to getBalance to try to cause a data race.
        wait(1000);

        // check if the amount of credits is enough
        if (newBalance < 0) {
            System.out.println("You can not afford that product.");
            return false;
        } else {
            // withdraw the price of the product from the wallet.
            wallet.setBalance(newBalance); 
            // add the name of the product to the pocket file.
            pocket.addProduct(product);
            // print the new balance.
            return true;
        }
    }

    private static void simulateRace(Wallet wallet, Pocket pocket) throws Exception {
        
        Thread t1 = new Thread(() -> {
            try {
                makePurchase("car", wallet, pocket); 
            } catch (Exception e) {
                System.err.println("Thread 1 error: " + e.getMessage());
            }
        });

        Thread t2 = new Thread(() -> {
            try {
                makePurchase("pen", wallet, pocket);
            } catch (Exception e) {
                System.err.println("Thread 2 error: " + e.getMessage());
            }
        });

        t1.start();
        t2.sleep(500);
        // Sleep for 0.5 s to make sure thread 1 calls getBalance before thread 2 starts
        t2.start();

        t1.join();
        t2.join();
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
