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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;

public class Server implements Runnable {
  private ServerSocket serverSocket;
  private Socket clientSocket;
  private final int PUERTO = 5000;
  private static List<PrintWriter> clientesWriter = new ArrayList<>();

  // Mapa de nodos: nodoId -> {ip, puerto, writer}
  private static Map<String, NodeInfo> nodosDisponibles = new ConcurrentHashMap<>();

  // Contador de tareas activas por nodo
  private static Map<String, AtomicInteger> cargaNodos = new ConcurrentHashMap<>();

  // Contadores para Round Robin
  private static volatile int roundRobinIndex = 0;
  private static List<String> nodosOrdenados = new ArrayList<>();

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
          int portNode = Integer.parseInt(parts[1]); // Corregido: era parts[1]

          NodeInfo nodeInfo = new NodeInfo("127.0.0.1", portNode, writer);
          nodosDisponibles.put(nodeId, nodeInfo);
          cargaNodos.put(nodeId, new AtomicInteger(0));

          // Actualizar lista ordenada para Round Robin
          synchronized (nodosOrdenados) {
            if (!nodosOrdenados.contains(nodeId)) {
              nodosOrdenados.add(nodeId);
            }
          }

          System.out.println("Nodo registrado: " + nodeId + " en puerto " + portNode);
        } else if (message.startsWith("HEARTBEAT:")) {
          // Heartbeat: HEARTBEAT:nodo_1
          String nodeId = message.split(":")[1];
          System.out.println("Heartbeat recibido de " + nodeId);
        } else if (message.startsWith("RESULTADO")) {
          // Resultado procesado por un nodo
          handleNodeResult(message);
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

  private static void handleNodeResult(String message) {
    // Extraer nodeId del resultado
    String[] parts = message.split(" -> ");
    if (parts.length >= 2) {
      String nodeId = parts[0].split(" ")[1]; // RESULTADO nodo_1 -> ...

      // Decrementar carga del nodo
      AtomicInteger carga = cargaNodos.get(nodeId);
      if (carga != null) {
        carga.decrementAndGet();
      }

      // Enviar resultado a clientes
      String resultado = parts[1];
      broadCast("Resultado: " + resultado);
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

    // Incrementar carga del nodo seleccionado
    cargaNodos.get(nodeId).incrementAndGet();

    // Procesar tarea de forma asíncrona para no bloquear el servidor
    new Thread(() -> {
      try (Socket nodoSocket = new Socket(node.getIp(), node.getPort());
           PrintWriter outToNode = new PrintWriter(nodoSocket.getOutputStream(), true);
           BufferedReader inFromNode = new BufferedReader(new InputStreamReader(nodoSocket.getInputStream()))) {

        // Enviar la tarea al nodo
        outToNode.println(mensaje);
        System.out.println("Tarea enviada a " + nodeId + ": " + mensaje);

        // Esperar el resultado del nodo
        String result = inFromNode.readLine();
        if (result != null) {
          System.out.println("Resultado del nodo " + nodeId + ": " + result);
          handleNodeResult(result);
        }

      } catch (IOException e) {
        System.err.println("Error al comunicarse con nodo " + nodeId + ": " + e.getMessage());
        // Decrementar carga en caso de error
        cargaNodos.get(nodeId).decrementAndGet();
        // Eliminar nodo si falla consistentemente
        nodosDisponibles.remove(nodeId);
        synchronized (nodosOrdenados) {
          nodosOrdenados.remove(nodeId);
        }
        cargaNodos.remove(nodeId);
      }
    }).start();
  }

  /**
   * Selecciona un nodo usando balanceo por carga
   * Si todos tienen la misma carga, usa Round Robin
   */
  private static String seleccionarNodo() {
    if (nodosDisponibles.isEmpty()) {
      return null;
    }

    // Encontrar el nodo con menor carga
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

    // Si no hay un claro ganador (misma carga), usar Round Robin
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

  // Método para mostrar estadísticas de carga (útil para debugging)
  public static void mostrarEstadisticas() {
    System.out.println("\n=== Estadísticas de Carga de Nodos ===");
    for (Map.Entry<String, AtomicInteger> entry : cargaNodos.entrySet()) {
      System.out.println(entry.getKey() + ": " + entry.getValue().get() + " tareas activas");
    }
    System.out.println("=====================================\n");
  }

  public static void main(String[] args) {
    new Thread(new Server()).start();

    // Opcional: Thread para mostrar estadísticas cada 5 segundos
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