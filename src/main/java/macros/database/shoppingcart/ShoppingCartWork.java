package macros.database.shoppingcart;

public interface ShoppingCartWork {

  /**
   * Create a new order and add 10 line items to it, for random products.
   * Then place the order.
   * These orders are pending.
   * 
   * Then, with a 20% chance and in another transaction, find all pending
   * orders and calculate the total price, and charge the customer.
   * These orders are charged.
   * 
   * Then, with a 10% chance, find all charged orders and ship them, thereby
   * reducing the product quantities.
   * These orders are shipped.
   */
  void doWork();

  /**
   * Close and release held resources.
   */
  void close();
}
