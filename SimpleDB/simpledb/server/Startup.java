package simpledb.server;

import simpledb.remote.*;

import java.rmi.RemoteException;
import java.rmi.registry.*;

public class Startup {
   public static void main(String args[]) throws Exception {
      // configure and initialize the database
      SimpleDB.init(args[0]);
      Registry reg;
      // create a registry specific for the server on the default port
      try{
    	   reg = LocateRegistry.createRegistry(1099);
      }
      catch(RemoteException e){
    	  reg = LocateRegistry.getRegistry(1099);
      }
      // and post the server entry in it
      RemoteDriver d = new RemoteDriverImpl();
      reg.rebind("simpledb", d);
      
      System.out.println("database server ready");
   }
}
