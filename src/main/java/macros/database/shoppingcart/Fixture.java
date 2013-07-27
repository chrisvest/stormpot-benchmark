package macros.database.shoppingcart;

import macros.database.Database;

enum Fixture {
  stormpot {
    @Override
    public ShoppingCartWork init(Database database, int poolSize) {
      return new StormpotShoppingCart(database.createDataSource(), poolSize);
    }
  },
  hibernate {
    @Override
    public ShoppingCartWork init(Database database, int poolSize) {
      return new HibernateShoppingCart(database, poolSize);
    }
  };
  
  public abstract ShoppingCartWork init(Database database, int poolSize);
}