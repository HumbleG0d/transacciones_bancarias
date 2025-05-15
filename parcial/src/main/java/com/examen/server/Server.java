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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Server implements Runnable {
  private ServerSocket serverSocket;
  private Socket clientSocket;
  private final int PUERTO = 5000;
  private static List<PrintWriter> clientesWriter = new ArrayList<>();
  private static Map<String, NodeInfo> nodosDisponibles = new ConcurrentHashMap<>();
  private static Map<String, AtomicInteger> cargaNodos = new ConcurrentHashMap<>();
  private static volatile int roundRobinIndex = 0;
  private static List<String> nodosOrdenados = new ArrayList<>();
  private static Map<String, List<String>> replicacionMap = new ConcurrentHashMap<>();

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
          String[] parts = message.split(":");
          if (parts.length < 4) {
            System.err.println("Formato inválido para REGISTRO_NODO: " + message);
            continue;
          }
          String nodeId = parts[1];
          String nodeIp = parts[2]; // Nueva IP del nodo
          int portNode = Integer.parseInt(parts[3]);
          NodeInfo nodeInfo = new NodeInfo(nodeIp, portNode, writer); // Usar la IP del nodo
          nodosDisponibles.put(nodeId, nodeInfo);
          cargaNodos.put(nodeId, new AtomicInteger(0));

          if (parts.length > 4) {
            String[] tablas = parts[3].split(",");
            synchronized (replicacionMap) {
              for (String tabla : tablas) {
                replicacionMap.computeIfAbsent(tabla, k -> new ArrayList<>()).add(nodeId);
              }
            }
          }

          synchronized (nodosOrdenados) {
            if (!nodosOrdenados.contains(nodeId)) {
              nodosOrdenados.add(nodeId);
            }
          }

          System.out.println("Nodo registrado: " + nodeId + " en puerto " + portNode + " con tablas: " + (parts.length > 3 ? parts[3] : "ninguna"));
        } else if (message.startsWith("HEARTBEAT:")) {
          String nodeId = message.split(":")[1];
          System.out.println("Heartbeat recibido de " + nodeId);
        } else if (message.startsWith("RESULTADO")) {
          handleNodeResult(message);
        } else if (message.startsWith("UPDATE:")) {
          propagarActualizacion(message);
        } else {
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

  private static void handleNodeResult(String message) {
    String[] parts = message.split(" -> ");
    if (parts.length >= 2) {
      String nodeId = parts[0].split(" ")[1];
      AtomicInteger carga = cargaNodos.get(nodeId);
      if (carga != null) {
        carga.decrementAndGet();
      }
      String resultado = parts[1];
      broadCast("Resultado: " + resultado);
    }
  }

  private static void propagarActualizacion(String updateMessage) {
    String[] parts = updateMessage.split(":", 4);
    if (parts.length < 4) return;

    String origenNodeId = parts[1];
    String tabla = parts[2];
    String contenido = parts[3];

    List<String> nodosReplicados = replicacionMap.getOrDefault(tabla, new ArrayList<>());
    for (String nodoId : nodosReplicados) {
      if (!nodoId.equals(origenNodeId)) {
        NodeInfo node = nodosDisponibles.get(nodoId);
        if (node != null) {
          PrintWriter outToNode = node.getWriter();
          outToNode.println("UPDATE:" + origenNodeId + ":" + tabla + ":" + contenido);
          outToNode.flush();
          System.out.println("Actualización enviada a " + nodoId + " para tabla " + tabla);
        }
      }
    }
  }

  private static void enviarTareaANodo(String mensaje) {
    if (nodosDisponibles.isEmpty()) {
      System.out.println("No hay nodos disponibles");
      return;
    }

    String nodeId = seleccionarNodo();
    if (nodeId == null) {
      System.out.println("No se pudo seleccionar un nodo");
      return;
    }

    NodeInfo node = nodosDisponibles.get(nodeId);
    cargaNodos.get(nodeId).incrementAndGet();

    new Thread(() -> {
      try (Socket nodoSocket = new Socket(node.getIp(), node.getPort());
           PrintWriter outToNode = new PrintWriter(nodoSocket.getOutputStream(), true);
           BufferedReader inFromNode = new BufferedReader(new InputStreamReader(nodoSocket.getInputStream()))) {

        outToNode.println(mensaje);
        System.out.println("Tarea enviada a " + nodeId + ": " + mensaje);

        String result = inFromNode.readLine();
        if (result != null) {
          System.out.println("Resultado del nodo " + nodeId + ": " + result);
          handleNodeResult(result);
        }

      } catch (IOException e) {
        System.err.println("Error al comunicarse con nodo " + nodeId + ": " + e.getMessage());
        cargaNodos.get(nodeId).decrementAndGet();
        nodosDisponibles.remove(nodeId);
        synchronized (nodosOrdenados) {
          nodosOrdenados.remove(nodeId);
        }
        cargaNodos.remove(nodeId);
      }
    }).start();
  }

  private static String seleccionarNodo() {
    if (nodosDisponibles.isEmpty()) {
      return null;
    }

    String nodoMenorCarga = null;
    int menorCarga = Integer.MAX_VALUE;

    for (Map.Entry<String, AtomicInteger> entry : cargaNodos.entrySet()) {
      String nodeId = entry.getKey();
      int carga = entry.getValue().get();

      if (nodosDisponibles.containsKey(nodeId) && carga < menorCarga) {
        menorCarga = carga;
        nodoMenorCarga = nodeId;
      }
    }

    if (nodoMenorCarga == null || menorCarga == getCargaPromedio()) {
      synchronized (nodosOrdenados) {
        if (!nodosOrdenados.isEmpty()) {
          roundRobinIndex = (roundRobinIndex + 1) % nodosOrdenados.size();
          return nodosOrdenados.get(roundRobinIndex);
        }
      }
    }

    return nodoMenorCarga;
  }

  private static int getCargaPromedio() {
    if (cargaNodos.isEmpty()) return 0;

    int total = 0;
    for (AtomicInteger carga : cargaNodos.values()) {
      total += carga.get();
    }
    return total / cargaNodos.size();
  }

  private static void broadCast(String message) {
    synchronized (clientesWriter) {
      for (PrintWriter clientWriter : clientesWriter) {
        clientWriter.println(message);
      }
    }
  }

  public static void mostrarEstadisticas() {
    System.out.println("\n=== Estadísticas de Carga de Nodos ===");
    for (Map.Entry<String, AtomicInteger> entry : cargaNodos.entrySet()) {
      System.out.println(entry.getKey() + ": " + entry.getValue().get() + " tareas activas");
    }
    System.out.println("=====================================\n");
  }

  public static void main(String[] args) {
    new Thread(new Server()).start();

    new Thread(() -> {
      while (true) {
        try {
          Thread.sleep(5000);
          mostrarEstadisticas();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }).start();
  }
}