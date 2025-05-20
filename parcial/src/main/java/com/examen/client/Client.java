package com.examen.client;

import com.examen.test.TransactionDataGenerator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
//Cada cliente genere automáticamente un conjunto de transacciones financieras y
// las envíe a un servidor a través de un socket.
public class Client implements Runnable {//Permite que cada cliente se ejecute como un hilo
  // independiente mediante new Thread(client).start().

  private final String name;
  private final int PORT = 5000;
  private final String HOST = "192.168.68.181";//Se conecta a un servidor en el puerto 5000 del host 127.0.0.1
  private static final int TRANSACTIONS_PER_CLIENT = 200; // Total 1002 transacciones (3 clientes)
  private static final long DELAY_BETWEEN_TRANSACTIONS_MS = 100; // 10 transacciones por segundo
//Se ejecutan tres clientes al mismo tiempo, en hilos separados, sumando un total de 1002 transacciones.

  public Client(String name) {
    this.name = name;
  }//Constructor de Clase Cliente

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
      new Thread(() -> {//Lee continuamente lo que dice el servidor (si responde).
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
      for (int i = 0; i < TRANSACTIONS_PER_CLIENT; i++) {//Se generan 334 transacciones usando
        // la clase TransactionDataGenerator.
        TransactionDataGenerator.Transaction transaction = TransactionDataGenerator.generateTransaction();
        String idCount = transaction.getIdCount();
        String idCountDestino = transaction.getIdCountDestino();
        String monto = String.format("%.2f", transaction.getMonto());//Monto de la transaccion


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
    //Crea tres instancias de clientes (Cliente1, Cliente2, Cliente3).
    Client client1 = new Client("Cliente1");
    Client client2 = new Client("Cliente2");
    Client client3 = new Client("Cliente3");
    //Inicia cada uno en su propio hilo, ejecutando sus transacciones simultáneamente.
    new Thread(client1).start();
    new Thread(client2).start();
    new Thread(client3).start();
  }
}