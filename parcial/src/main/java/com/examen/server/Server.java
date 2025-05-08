package com.examen.server;

import com.examen.nodes.NodeInfo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Server implements Runnable {
  private ServerSocket serverSocket;
  private Socket clientSocket;
  private final int PUERTO = 5000;
  private static List<PrintWriter> clientesWriter = new ArrayList<>();
  // Mapa de nodos: nodoId -> {ip, puerto, writer}
  private static Map<String, NodeInfo> nodosDisponibles = new HashMap<>();

  @Override
  public void run() {
    try {
      serverSocket = new ServerSocket(PUERTO);
      System.out.println("Servidor iniciado correctamente");

      while (true) {
        clientSocket = serverSocket.accept();
        System.out.println(clientSocket.getInetAddress() + " conectado al servidor");
        PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true);
        new Thread(() -> handleConnection(clientSocket, writer)).start();
      }
    } catch (IOException e) {
      System.err.println(e);
    }
  }

  private static void handleConnection(Socket socket, PrintWriter writer) {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
      String message;
      while ((message = reader.readLine()) != null) {
        System.out.println("Mensaje recibido: " + message);

        if (message.startsWith("REGISTRO_NODO")) {
          // Registro de un nodo: REGISTRO_NODO:nodo_1:6000
          String[] parts = message.split(":");
          String nodeId = parts[1];
          int portNode = Integer.parseInt(parts[1]);
          nodosDisponibles.put(nodeId, new NodeInfo("127.0.0.1", portNode, writer));
          System.out.println("Nodo registrado: " + nodeId);
        } else if (message.startsWith("HEARTBEAT:")) {
          // Heartbeat: HEARTBEAT:nodo_1
          String nodeId = message.split(":")[1];
          System.out.println("Heartbeat recibido de " + nodeId);
        } else if (message.startsWith("RESULTADO")) {
          // Resultado de un nodo: RESULTADO:nodo_1:Éxito: Saldo origen=1000.00, Saldo destino=3700.50
          String resultado = message.substring(message.indexOf(":", message.indexOf(":") + 1) + 1);
          broadCast("Resultado: " + resultado);
        } else {
          // Mensaje de un cliente, enviarlo a un nodo
          clientesWriter.add(writer);
          enviarTareaANodo(message);
        }
      }
    } catch (IOException e) {
      System.err.println("Error al recibir mensajes " + e.getMessage());
    } finally {
      clientesWriter.remove(writer);
    }
  }

  private static void enviarTareaANodo(String mensaje) {
    // Elegir un nodo (simplificado: usar el primero disponible)
    if (nodosDisponibles.isEmpty()) {
      System.out.println("No hay nodos disponibles");
      return;
    }
    String nodeId = nodosDisponibles.keySet().iterator().next();
    NodeInfo node = nodosDisponibles.get(nodeId);

    try (Socket nodoSocket = new Socket(node.getIp(), node.getPort());
         PrintWriter outToNode = new PrintWriter(nodoSocket.getOutputStream(), true);
         BufferedReader inFromNode = new BufferedReader(new InputStreamReader(nodoSocket.getInputStream()))) {

      // Enviar la tarea al nodo
      outToNode.println(mensaje);

      // Esperar el resultado del nodo
      String result = inFromNode.readLine();
      System.out.println("Resultado del nodo " + nodeId + ": " + result);

      // Extraer el resultado y enviarlo a los clientes
      if (result != null && result.startsWith("RESULTADO")) {
        String resultado = result.substring(result.indexOf(":", result.indexOf(":") + 1) + 1);
        broadCast("Resultado: " + resultado);
      }

    } catch (IOException e) {
      System.err.println("Error al enviar tarea al nodo " + nodeId + ": " + e.getMessage());
      nodosDisponibles.remove(nodeId); // Eliminar nodo si falla
    }
  }

  private static void broadCast(String message) {
    for (PrintWriter clientWriter : clientesWriter) {
      clientWriter.println(message);
    }
  }

  public static void main(String[] args) {
    new Thread(new Server()).start();
  }
}