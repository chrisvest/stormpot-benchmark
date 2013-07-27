package macros.database.shoppingcart;

public interface ShoppingCartWork {

  /**
   * Create a new order and add 10 line items to it, for random products.
   * Then place the order, and update the product availabilities.
   */
  void doWork();

  void close();
}
