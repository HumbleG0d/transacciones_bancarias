package com.examen.client;

import com.examen.test.TransactionDataGenerator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class Client implements Runnable {
  private final String name;
  private final int PORT = 5000;
  private final String HOST = "127.0.0.1";
  private static final int TRANSACTIONS_PER_CLIENT = 334; // Total 1002 transacciones (3 clientes)
  private static final long DELAY_BETWEEN_TRANSACTIONS_MS = 100; // 10 transacciones por segundo

  public Client(String name) {
    this.name = name;
  }

  @Override
  public void run() {
    try (
            // Estableciendo conexión con el servidor
            Socket clientSocket = new Socket(HOST, PORT);
            // Para enviar mensajes al servidor
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
            // Para recibir mensajes del servidor
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))
    ) {
      // Hilo para recibir mensajes del servidor
      new Thread(() -> {
        try {
          String serverMessage;
          while ((serverMessage = in.readLine()) != null) {
            System.out.println(name + " recibió: " + serverMessage);
          }
        } catch (IOException e) {
          System.err.println(name + " desconectado del servidor: " + e.getMessage());
        }
      }).start();

      // Generar y enviar transacciones automáticamente
      for (int i = 0; i < TRANSACTIONS_PER_CLIENT; i++) {
        TransactionDataGenerator.Transaction transaction = TransactionDataGenerator.generateTransaction();
        String idCount = transaction.getIdCount();
        String idCountDestino = transaction.getIdCountDestino();
        String monto = String.format("%.2f", transaction.getMonto());
        out.println("2-" + idCount + ":" + idCountDestino + ":" + monto);
        System.out.println(name + " envió transacción #" + (i + 1) + ": " + idCount + " -> " + idCountDestino + " por " + monto);

        // Esperar para mantener ~10 transacciones por segundo
        try {
          Thread.sleep(DELAY_BETWEEN_TRANSACTIONS_MS);
        } catch (InterruptedException e) {
          System.err.println(name + " interrumpido durante espera: " + e.getMessage());
        }
      }

      System.out.println(name + " completó todas las transacciones.");

    } catch (IOException e) {
      System.err.println("Error en el cliente " + name + ": " + e.getMessage());
    }
  }

  public static void main(String[] args) {
    // Crear 3 clientes
    Client client1 = new Client("Cliente1");
    Client client2 = new Client("Cliente2");
    Client client3 = new Client("Cliente3");

    // Iniciar cada cliente en un hilo separado
    new Thread(client1).start();
    new Thread(client2).start();
    new Thread(client3).start();
  }
}