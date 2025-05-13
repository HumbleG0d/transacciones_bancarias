package com.examen.nodes;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class Nodo implements Runnable {

  private final String nodeId;
  private final int portNode;
  private final String serverHost = "127.0.0.1";
  private final int serverPort = 5000;
  private ServerSocket server;
  private Socket socket; //socker para conectarce con el sevidor principal
  private BufferedReader inFromServer; //Para enviar mensajes del servidor
  private PrintWriter outToServer; //Para recibir mensajes del servidor
  private String root_directory;
  private String[] table_counts = {"cu_1.txt" , "cu_2.txt" , "cu_3.txt"};

  public Nodo(String nodeId, int portNode , String root_directory) {
    this.nodeId = nodeId;
    this.portNode = portNode;
    this.root_directory = root_directory;
  }


  @Override
  public void run() {
    try {
      //Conexion del nodo al servidor central
      connectionServer();
      registerNode();

      //Notificar estatus del nodo


      //Inicial el nodo para escuchar tareas
      server = new ServerSocket(portNode);
      System.out.println("Nodo " + nodeId + " inciado en el puerto " + portNode);

      while (true) {
        Socket socketTask = server.accept();
        System.out.println("Nodo " + nodeId + " recibio una tarea de " + socketTask.getInetAddress());
        new Thread(() -> handleTask(socketTask)).start();
      }
    } catch (IOException e) {
      System.err.println("Error al crear el nodo " + nodeId + ": " + e.getMessage());
    }
  }

  private void connectionServer() throws IOException {
    socket = new Socket(serverHost, serverPort);
    outToServer = new PrintWriter(socket.getOutputStream(), true);
    inFromServer = new BufferedReader(new InputStreamReader(socket.getInputStream()));
  }

  private void registerNode() {
    outToServer.println("REGISTRO_NODO" + nodeId + ":" + portNode);
  }

  //Maneja una tarea recibida por el servidor central
  private void handleTask(Socket taskSocket) {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(taskSocket.getInputStream()));
         PrintWriter writer = new PrintWriter(taskSocket.getOutputStream(), true)) {

      String option = reader.readLine();
//      System.out.println("Nodo " + nodeId + " recibio tarea:  " + message);

      if (option.startsWith("1")) {
        //Simular tarea
        String id_count = option.split("-")[1];
        String result = checkBalance(id_count);
        writer.println("RESULTADO " + nodeId + " -> " + result);
      }

    } catch (IOException e) {
      System.err.println("Error al manejar la tarea en el nodo" + nodeId + ": " + e.getMessage());
    }
  }

  private String checkBalance(String idClient) {
    int x = 0;
    while (x < table_counts.length) {
      try {
        File file = new File(root_directory + table_counts[x]);
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line;
        br.readLine();
        br.readLine();

        while ((line = br.readLine()) != null) {
          String[] tokens = line.split("\\|");

          String id_count = tokens[0].trim();
          outToServer.println(tokens[0]);
          if (id_count.equals(idClient)) {
            br.close();
            return "SALDO: " + tokens[2];
          }
        }
      } catch (IOException e) {
        return "Error al leer el nodo " + nodeId + ": " + e.getMessage();
      }
      x++;
    }
    return "CUENTA NO ENCONTRADA";
  }
}
