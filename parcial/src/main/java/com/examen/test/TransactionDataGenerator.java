package com.examen.test;

import java.util.Random;
//Simula una transacción entre dos cuentas bancarias diferentes.
//Genera una cuenta origen, una cuenta destino y un monto aleatorio entre 10.0 y 500.0.
//Asegura que la cuenta origen y destino no sean iguales.
//Está diseñada para ser reutilizada donde se necesiten datos de prueba para transferencias financieras.


public class TransactionDataGenerator {
  //ACCOUNTS: Lista de cuentas predefinidas (simuladas).
  private static final String[] ACCOUNTS = {"101", "102", "103", "104", "105" , "202", "223", "244", "265", "202", "203", "208", "125"}; // Cuentas predefinidas
  //  RANDOM: Generador de números aleatorios.
  private static final Random RANDOM = new Random();
  private static final double MIN_AMOUNT = 10.0; // Monto mínimo
  private static final double MAX_AMOUNT = 500.0; // Monto máximo

  public static class Transaction {
    private final String idCount;
    private final String idCountDestino;
    private final double monto;

    public Transaction(String idCount, String idCountDestino, double monto) {
      this.idCount = idCount;//idCount: ID de la cuenta de origen.
      this.idCountDestino = idCountDestino;//idCountDestino: ID de la cuenta de destino.
      this.monto = monto;//monto: cantidad de dinero que se transfiere.
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
      //Selecciona una cuenta de origen aleatoria del arreglo ACCOUNTS.
      idCountDestino = ACCOUNTS[RANDOM.nextInt(ACCOUNTS.length)];
    } while (idCount.equals(idCountDestino)); //Realiza el bucle mientras el idCount sea igual an idCountDest
    // Asegurar que sean diferentes

    // Generar monto aleatorio
    double monto = MIN_AMOUNT + (MAX_AMOUNT - MIN_AMOUNT) * RANDOM.nextDouble();
    return new Transaction(idCount, idCountDestino, monto);
  }
}
