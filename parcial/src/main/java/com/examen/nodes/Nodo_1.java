package com.examen.nodes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class Nodo_1 implements Runnable {

  private final String nodeId;
  private final int portNode;
  private final String serverHost = "127.0.0.1";
  private final int serverPort = 5000;
  private ServerSocket server;
  private Socket socket; //socker para conectarce con el sevidor principal
  private BufferedReader inFromServer; //Para enviar mensajes del servidor
  private PrintWriter outToServer; //Para recibir mensajes del servidor


  public Nodo_1(String nodeId, int portNode) {
    this.nodeId = nodeId;
    this.portNode = portNode;
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

      String message = reader.readLine();
      System.out.println("Nodo " + nodeId + " recibio tarea:  " + message);


      if(message.startsWith("OPcion:1")){
      //Simular tarea
      String result = processTask(message);
        writer.println("RESULTADO " + nodeId + " -> " + result);
      }

    } catch (IOException e) {
      System.err.println("Error al manejar la tarea en el nodo" + nodeId + ": " + e.getMessage());
    }
  }

  //Simular procesamiento de una tarea
  private String processTask(String message) {
    return "GAAAA";
  }

  public static void main(String[] args) {
    new Thread(new Nodo_1("nodo_1", 6000)).start();
  }

}
