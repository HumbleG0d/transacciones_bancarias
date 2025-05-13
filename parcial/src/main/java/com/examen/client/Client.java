package com.examen.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

public class Client implements Runnable {
  private String name;
  private final int PORT = 5000;
  private final String HOST = "127.0.0.1";

  public Client(String name) {
    this.name = name;
  }


  @Override
  public void run() {
    try(
            //Estableciendo conexion con el servidor
            Socket clientSocket = new Socket(HOST , PORT);
            //Para enviar mensajes al servidor
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream() , true);
            //Para recibir mensaje del servidor
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
    ){
      Scanner sc = new Scanner(System.in);

      //Hilo para recibir mensaje
      new Thread(() ->{
        try{
          String serverMessage;
          while((serverMessage = in.readLine()) != null){
            System.out.println(serverMessage);
          }
        }catch (IOException e){
          System.err.println("Desconectado del servidor");
        }
      }).start();

      //Hilo principal para enviar mensajes
      while (true) {
        System.out.println("""
                OPCIÓN 1 -> LECTURA DE CUENTA SALDO
                OPCIÓN 2 -> TRANSFERENCIA DE CUENTA SALDO
                """);
        System.out.print("OPCIÓN: ");
        String userInput = sc.nextLine();

        if (userInput.startsWith("1")) {
          System.out.print("CUENTA SALDO: ");
          String id_count = sc.nextLine();
          out.println("1-" + id_count);
        } else if (userInput.startsWith("2")) {
          System.out.print("CUENTA SALDO ORIGEN: ");
          String id_count = sc.nextLine();
          System.out.print("CUENTA SALDO DESTINO: ");
          String id_count_destino = sc.nextLine();
          System.out.print("MONTO: ");
          String monto = sc.nextLine();
          out.println("2-" + id_count + ":" + id_count_destino + ":" + monto);
        } else {
          System.out.println("Opción no válida. Intente nuevamente.");
        }
      }

    }catch (IOException e){
      System.err.println("Error en el cliente: " + e.getMessage());
    }
  }

  public static void main(String[] args) {
    Client client = new Client("Sideral");
    new Thread(client).start();
  }
}
