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

    public static void main(String[] args) throws Exception {
        Wallet wallet = new Wallet();
        Pocket pocket = new Pocket();
        Scanner scanner = new Scanner(System.in);

        print(wallet, pocket);
        String product = scan(scanner);

        while(!product.equals("quit")) {

            Integer price = Store.getProductPrice(product);

            // check if the amount of credits is enough
            // Integer balance = wallet.getBalance();
            boolean withdrawal = wallet.safeWithdraw(price);
            if (!withdrawal) {
                // If not stop the execution.
                System.out.println("You can not afford that product.");
                break;
            }
            else {
                // withdraw the price of the product from the wallet.
                // wallet.setBalance(balance - price);

                // add the name of the product to the pocket file.
                pocket.addProduct(product);
            }

            // Just to print everything again...
            print(wallet, pocket);
            product = scan(scanner);
        }
    }
}
