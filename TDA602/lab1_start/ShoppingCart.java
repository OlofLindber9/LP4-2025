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

    private static void wait(int ms) {
        try {
            Thread.sleep(ms);
        } catch (Exception e) {
        }
    }

    public static void main(String[] args) throws Exception {
        Wallet wallet = new Wallet();
        Pocket pocket = new Pocket();
        Scanner scanner = new Scanner(System.in);

        print(wallet, pocket);
        String product = scan(scanner);

        while(!product.equals("quit")) {

            if(product.equals("simulateRace")) {
                Thread buyer1 = new Thread(() -> {
                    try {
                        boolean result = wallet.safeWithdraw(30000);  // assuming 100 credits
                        System.out.println("Buyer1 withdrawal successful? " + result);
                        if(result) {
                            pocket.addProduct("car");
                        }
                    } catch (Exception e) {
                        System.err.println("Buyer1 error: " + e.getMessage());
                    }
                });
            
                Thread buyer2 = new Thread(() -> {
                    try {
                        boolean result = wallet.safeWithdraw(30000);
                        System.out.println("Buyer2 withdrawal successful? " + result);
                        if(result) {
                            pocket.addProduct("car");
                        }
                    } catch (Exception e) {
                        System.err.println("Buyer2 error: " + e.getMessage());
                    }
                });
                
                buyer1.start();
                buyer2.start();
                
                buyer1.join();
                buyer2.join();

                // Just to print everything again...
                print(wallet, pocket);
                product = scan(scanner);
                break;
            } else {
            
            /* TODO:
               - check if the amount of credits is enough, if not stop the execution.
               - otherwise, withdraw the price of the product from the wallet.
               - add the name of the product to the pocket file.
               - print the new balance.
            */
            
            int price = Store.getProductPrice(product);
            if(!wallet.safeWithdraw(price)){
                System.out.println("medjes ej");
            } else{
                wait(5000);
                pocket.addProduct(product);
            }
            // Just to print everything again...
            print(wallet, pocket);
            product = scan(scanner);
            }
        }
    }
}
