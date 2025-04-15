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
            try {
                int newBalance = wallet.getBalance() - Store.getProductPrice(product);

                // Sleep for 5 s to simulate data race
                wait(5000);

                // - check if the amount of credits is enough
                if (newBalance < 0) {
                    // if not stop the execution.
                    System.out.println("You can not afford that product.");
                } else {
                    // - otherwise, withdraw the price of the product from the wallet.
                    wallet.setBalance(newBalance);
                    // - add the name of the product to the pocket file.
                    pocket.addProduct(product);
                    // - print the new balance.
                }
            }
            catch (IllegalArgumentException e) {
                System.out.println(e.getMessage());
                // break;
            }

            // Just to print everything again...
            print(wallet, pocket);
            product = scan(scanner);
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
