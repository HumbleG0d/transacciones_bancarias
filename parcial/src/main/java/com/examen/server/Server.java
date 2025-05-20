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

//Este código define un servidor multihilo en Java que actúa como coordinador de tareas distribuidas
// entre varios nodos. Los nodos se registran en el servidor, y el servidor distribuye tareas entre ellos,
// monitorea su carga y replica actualizaciones entre los nodos según tablas asignadas.

public class Server implements Runnable {
  private ServerSocket serverSocket;
  private Socket clientSocket;
  private final int PUERTO = 5000;

  //Guarda la salida hacia los clientes conectados para poder enviarles resultados o mensajes.
  private static List<PrintWriter> clientesWriter = new ArrayList<>();

  //Registra todos los nodos que se han conectado al servidor
  private static Map<String, NodeInfo> nodosDisponibles = new ConcurrentHashMap<>();

  // Registra cuántas tareas está ejecutando cada nodo (carga actual).
  private static Map<String, AtomicInteger> cargaNodos = new ConcurrentHashMap<>();


  private static volatile int roundRobinIndex = 0;

  //Lista ordenada de nodos para aplicar Round Robin como estrategia de balanceo.
  private static List<String> nodosOrdenados = new ArrayList<>();

  //Mapa de tablas a nodos que replican esa tabla.
  private static Map<String, List<String>> replicacionMap = new ConcurrentHashMap<>();

  @Override
  public void run() {//Este método inicia el servidor y acepta conexiones entrantes en un bucle infinito

    try {
      serverSocket = new ServerSocket(PUERTO);
      System.out.println("Servidor iniciado correctamente");
      //
      while (true) {//Cada conexión entrante lanza un hilo nuevo para manejar al
        // cliente con handleConnection.
        clientSocket = serverSocket.accept();
        System.out.println(clientSocket.getInetAddress() + " conectado al servidor");
        PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true);
        new Thread(() -> handleConnection(clientSocket, writer)).start();
      }
    } catch (IOException e) {
      System.err.println(e);
    }
  }

  //Este método maneja los mensajes que recibe el servidor de un cliente o nodo.
  private static void handleConnection(Socket socket, PrintWriter writer) {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
      String message;
      while ((message = reader.readLine()) != null) {
        System.out.println("Mensaje recibido: " + message);

        //REGISTRO_NODO: un nodo se registra. Se almacena su IP, puerto y las tablas que maneja.
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
            String[] tablas = parts[4].split(",");
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
        } else if (message.startsWith("HEARTBEAT:")) {//HEARTBEAT: señal de vida desde un nodo.
          String nodeId = message.split(":")[1];
          System.out.println("Heartbeat recibido de " + nodeId);
        } else if (message.startsWith("RESULTADO")) {//RESULTADO: respuesta del nodo después de ejecutar una
          // tarea.
          handleNodeResult(message);
        } else if (message.startsWith("UPDATE:")) {//UPDATE:: propagación de actualización (replicación).
          propagarActualizacion(message);
        } else {//Tarea de cliente: si no es un comando especial, se considera una tarea y
          // se distribuye a un nodo.
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

  private static void handleNodeResult(String message) {//Recibe un mensaje de resultado:
    // La función toma como parámetro un String message
    // que contiene el resultado enviado por un nodo.

//Divide el mensaje: Separa el mensaje usando el delimitador -> para extraer
// la identificación del nodo y el resultado.
    String[] parts = message.split(" -> ");
    if (parts.length >= 2) {
      //Por ejemplo, si el mensaje es "Nodo 101 -> Resultado", entonces nodeId será "101".
      String nodeId = parts[0].split(" ")[1];

      //Actualiza la carga del nodo: Busca el contador atómico (AtomicInteger) asociado con ese nodo
      // en el mapa cargaNodos y decrementa su valor.
      AtomicInteger carga = cargaNodos.get(nodeId);

      if (carga != null) {
        carga.decrementAndGet();
      }
      String resultado = parts[1];
      //Envía el resultado a todos los clientes mediante broadCast.
      broadCast("Resultado: " + resultado);
    }
  }

  private static void propagarActualizacion(String updateMessage) {
    //Propaga actualizaciones (UPDATE) a todos los nodos que replican la misma tabla,
    // excepto el nodo que originó el cambio.
    //Recibe un mensaje de actualización: Toma como parámetro un String updateMessage que
    // contiene la información sobre una actualización.

    //Divide el mensaje en partes usando ":" como delimitador, limitando a 4 partes máximo.
    //El formato esperado es: UPDATE:origenNodeId:tabla:contenido
    String[] parts = updateMessage.split(":", 4);

    //Validación básica: Comprueba que el mensaje tenga el formato correcto.
    if (parts.length < 4) return;

    String origenNodeId = parts[1];// ID del nodo que generó la actualización
    String tabla = parts[2];// Tabla que fue actualizada
    String contenido = parts[3];// Contenido de la actualización

    //dentifica los nodos que replican la tabla: Obtiene la lista de nodos que mantienen una réplica
    // de la tabla específica.
    List<String> nodosReplicados = replicacionMap.getOrDefault(tabla, new ArrayList<>());
    if (nodosReplicados.isEmpty()) {
      System.out.println("No hay nodos replicados para la tabla: " + tabla);

    }
    //Envía la actualización a cada nodo replicado: Itera sobre todos los nodos que replican la tabla, excluyendo el
    // nodo que originó la actualización.
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


  //Este metodo enviarTareaANodo implementa la distribución de tareas a nodos en un sistema distribuido.
  private static void enviarTareaANodo(String mensaje) {
    //Comprueba si hay nodos disponibles para procesar tareas.
    if (nodosDisponibles.isEmpty()) {
      System.out.println("No hay nodos disponibles");
      return;
    }

    //Llama a un método seleccionarNodo que probablemente implementa alguna estrategia de
    // balanceo de carga para elegir el nodo más adecuado.
    String nodeId = seleccionarNodo();

    if (nodeId == null) {
      System.out.println("No se pudo seleccionar un nodo");
      return;
    }
//Obtiene la información del nodo seleccionado e incrementa atómicamente su contador de carga, indicando
// que está procesando una tarea adicional.
    NodeInfo node = nodosDisponibles.get(nodeId);
    cargaNodos.get(nodeId).incrementAndGet();

    //Crea y arranca un nuevo hilo para manejar la comunicación con el nodo, permitiendo
    // que el hilo principal continúe sin bloquearse.
    new Thread(() -> {
      //Abre una conexión socket con el nodo seleccionado y configura flujos para enviar y recibir datos.
      try (Socket nodoSocket = new Socket(node.getIp(), node.getPort());
           PrintWriter outToNode = new PrintWriter(nodoSocket.getOutputStream(), true);
           BufferedReader inFromNode = new BufferedReader(new InputStreamReader(nodoSocket.getInputStream()))) {

        //Envía el mensaje de tarea al nodo y registra la acción.
        outToNode.println(mensaje);
        System.out.println("Tarea enviada a " + nodeId + ": " + mensaje);

        //Espera y lee la respuesta del nodo, y si recibe un resultado, lo procesa llamando a
        // la función handleNodeResult() que vimos anteriormente.
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


  //Este método seleccionarNodo() tiene como objetivo elegir un nodo disponible del sistema,
  // siguiendo criterios de
  // balanceo de carga, es decir, distribuir el trabajo entre nodos de forma eficiente.
  private static String seleccionarNodo() {

    //Si no hay nodos disponibles, devuelve null.
    if (nodosDisponibles.isEmpty()) {
      return null;
    }

    String nodoMenorCarga = null;
    int menorCarga = Integer.MAX_VALUE;

    //Recorre el mapa cargaNodos, que contiene el identificador del nodo (String) y su carga (AtomicInteger).
    for (Map.Entry<String, AtomicInteger> entry : cargaNodos.entrySet()) {
      String nodeId = entry.getKey();
      int carga = entry.getValue().get();

      if (nodosDisponibles.containsKey(nodeId) && carga < menorCarga) {
        menorCarga = carga;
        nodoMenorCarga = nodeId;//Guarda el nodo con menor carga
      }
    }

    //Si no encuentra un nodo de menor carga o tiene la misma carga que el promedio,
    // usa round-robin (orden circular) como mecanismo de respaldo:
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
