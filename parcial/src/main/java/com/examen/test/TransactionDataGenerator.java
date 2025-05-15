package com.examen.test;

import java.util.Random;

public class TransactionDataGenerator {
  private static final String[] ACCOUNTS = {"101", "102", "103", "104", "105" , "202", "223", "244", "265", "202", "203", "164", "125"}; // Cuentas predefinidas
  private static final Random RANDOM = new Random();
  private static final double MIN_AMOUNT = 10.0; // Monto mínimo
  private static final double MAX_AMOUNT = 500.0; // Monto máximo

  public static class Transaction {
    private final String idCount;
    private final String idCountDestino;
    private final double monto;

    public Transaction(String idCount, String idCountDestino, double monto) {
      this.idCount = idCount;
      this.idCountDestino = idCountDestino;
      this.monto = monto;
    }

    public String getIdCount() {
      return idCount;
    }

    public String getIdCountDestino() {
      return idCountDestino;
    }

    public double getMonto() {
      return monto;
    }
  }

  public static Transaction generateTransaction() {
    // Seleccionar cuenta origen y destino (diferentes)
    String idCount = ACCOUNTS[RANDOM.nextInt(ACCOUNTS.length)];
    String idCountDestino;
    do {
      idCountDestino = ACCOUNTS[RANDOM.nextInt(ACCOUNTS.length)];
    } while (idCount.equals(idCountDestino)); // Asegurar que sean diferentes

    // Generar monto aleatorio
    double monto = MIN_AMOUNT + (MAX_AMOUNT - MIN_AMOUNT) * RANDOM.nextDouble();
    return new Transaction(idCount, idCountDestino, monto);
  }
}
