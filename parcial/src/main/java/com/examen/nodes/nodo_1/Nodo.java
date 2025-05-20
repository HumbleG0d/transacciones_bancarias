
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.net.InetAddress;

public class Nodo implements Runnable {

  private final String nodeId;
  private final int portNode;
  private final String serverHost = "192.168.68.181";
  private final int serverPort = 5000;
  private ServerSocket server;
  private Socket socket;
  private BufferedReader inFromServer;
  private PrintWriter outToServer;
  private String root_directory;
  private String[] table_counts = {"cu_1.txt", "cu_2.txt", "cu_3.txt"};
  private final Object fileLock = new Object();
  private final Object transactionLock = new Object();
  private int transactionId = 0;

  public Nodo(String nodeId, int portNode, String root_directory) {
    this.nodeId = nodeId;
    this.portNode = portNode;
    this.root_directory = root_directory.endsWith(File.separator) ? root_directory : root_directory + File.separator;
    initializeTransactionFile();
  }

  private void initializeTransactionFile() {
    File transactionFile = new File(root_directory + "transacciones.txt");
    if (!transactionFile.exists()) {
      try (FileWriter fw = new FileWriter(transactionFile)) {
        fw.write("ID_TRANSACC | ID_ORIG | ID_DEST | MONTO  | FECHA_HORA         | ESTADO\n");
        fw.write("----------------------------------------------------------------------------\n");
        fw.flush();
        System.out.println("Archivo transacciones.txt creado con encabezado a las " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
      } catch (IOException e) {
        System.err.println("Error al crear transacciones.txt: " + e.getMessage());
      }
    } else {
      try (BufferedReader br = new BufferedReader(new FileReader(transactionFile))) {
        String line;
        br.readLine();
        br.readLine();
        while ((line = br.readLine()) != null) {
          String[] tokens = line.split("\\|");
          if (tokens.length >= 1) {
            try {
              transactionId = Integer.parseInt(tokens[0].trim());
            } catch (NumberFormatException e) {
              System.err.println("Error al parsear ID_TRANSACC en transacciones.txt: " + tokens[0].trim());
            }
          }
        }
        System.out.println("Archivo transacciones.txt existente. Último ID_TRANSACC leído: " + transactionId + " a las " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
      } catch (IOException e) {
        System.err.println("Error al leer transacciones.txt: " + e.getMessage());
      }
    }
  }

  @Override
  public void run() {
    try {
      connectionServer();
      registerNode();
      server = new ServerSocket(portNode);
      System.out.println("Nodo " + nodeId + " iniciado en el puerto " + portNode + " a las " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")));

      new Thread(this::handleTasks).start();
      new Thread(this::handleUpdates).start();

    } catch (IOException e) {
      System.err.println("Error al crear el nodo " + nodeId + ": " + e.getMessage());
    }
  }

  private void connectionServer() throws IOException {
    socket = new Socket(serverHost, serverPort);
    outToServer = new PrintWriter(socket.getOutputStream(), true);
    inFromServer = new BufferedReader(new InputStreamReader(socket.getInputStream()));
  }

  private void registerNode() throws UnknownHostException {
    String tablasReplicadas = String.join(",", table_counts);
    String ip = InetAddress.getLocalHost().getHostAddress(); // Obtener la IP del nodo
    outToServer.println("REGISTRO_NODO:" + nodeId + ":" + ip + ":" + portNode + ":" + tablasReplicadas);
    System.out.println("Nodo " + nodeId + " registrado en el servidor con IP " + ip + " y tablas: " + tablasReplicadas + " a las " + java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss")));
  }

  private void handleTasks() {
    try {
      while (true) {
        Socket socketTask = server.accept();
        System.out.println("Nodo " + nodeId + " recibió una tarea de " + socketTask.getInetAddress() + " a las " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
        new Thread(() -> handleTask(socketTask)).start();
      }
    } catch (IOException e) {
      System.err.println("Error al aceptar tareas en " + nodeId + ": " + e.getMessage());
    }
  }

  private void handleTask(Socket taskSocket) {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(taskSocket.getInputStream()));
         PrintWriter writer = new PrintWriter(taskSocket.getOutputStream(), true)) {

      String option = reader.readLine();

      if (option != null) {
        if (option.startsWith("1")) {
          String id_count = option.split("-")[1];
          String result = checkBalance(id_count);
          writer.println("RESULTADO " + nodeId + " -> " + result);
        } else if (option.startsWith("2")) {
          String[] tokens = option.split("-");
          String[] details = tokens[1].split(":");
          String idOrigen = details[0];
          String idDestino = details[1];
          String montoStr = details[2];
          String result = transferBalance(idOrigen, idDestino, montoStr);
          writer.println("RESULTADO " + nodeId + " -> TRANSFERENCIA DE " + idOrigen + " A " + idDestino + " POR " + montoStr + " - " + result);
        }
      }
    } catch (IOException e) {
      System.err.println("Error al manejar la tarea en el nodo " + nodeId + ": " + e.getMessage());
    }
  }

  private String checkBalance(String idClient) {
    for (String fileName : table_counts) {
      try {
        File file = new File(root_directory + fileName);
        if (!file.exists()) {
          return "Archivo no encontrado: " + fileName;
        }
        System.out.println("Leyendo archivo para checkBalance: " + file.getAbsolutePath() + " a las " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
          br.readLine();
          br.readLine();
          String line;
          while ((line = br.readLine()) != null) {
            String[] tokens = line.split("\\|");
            if (tokens.length >= 3) {
              String id_count = tokens[0].trim();
              if (id_count.equals(idClient)) {
                String saldoStr = tokens[2].trim().replace(",", "");
                return "SALDO: " + saldoStr;
              }
            }
          }
        }
      } catch (IOException e) {
        return "Error al leer el nodo " + nodeId + ": " + e.getMessage();
      }
    }
    return "CUENTA NO ENCONTRADA";
  }

  private void logTransaction(String idOrigen, String idDestino, double monto, String estado) {
    synchronized (transactionLock) {
      transactionId++;
      LocalDateTime now = LocalDateTime.now();
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
      String fechaHora = now.format(formatter);

      try (FileWriter fw = new FileWriter(root_directory + "transacciones.txt", true)) {
        String line = String.format("%-10d | %-7s | %-7s | %-7.2f | %-19s | %s\n",
                transactionId, idOrigen, idDestino, monto, fechaHora, estado);
        fw.write(line);
        fw.flush();
        System.out.println("Transacción registrada y escrita: ID_TRANSACC=" + transactionId + ", ID_ORIG=" + idOrigen +
                ", ID_DEST=" + idDestino + ", MONTO=" + monto + ", FECHA_HORA=" + fechaHora + ", ESTADO=" + estado + " a las " + now.format(DateTimeFormatter.ofPattern("HH:mm:ss")));
      } catch (IOException e) {
        System.err.println("Error al registrar la transacción: " + e.getMessage() + " a las " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
      }
    }
  }

  private String transferBalance(String idOrigen, String idDestino, String amount) {
    System.out.println("TRANSFERENCIA: " + idOrigen + " " + idDestino + " " + amount + " a las " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
    double monto;
    try {
      monto = Double.parseDouble(amount);
    } catch (NumberFormatException e) {
      logTransaction(idOrigen, idDestino, 0.0, "Pendiente");
      return "Error al parsear el monto: " + e.getMessage();
    }

    String origenFile = null;
    String destinoFile = null;
    double saldoOrigen = 0.00;
    double saldoDestino = 0.00;

    for (String fileName : table_counts) {
      try {
        File file = new File(root_directory + fileName);
        if (!file.exists()) {
          logTransaction(idOrigen, idDestino, monto, "Pendiente");
          return "Archivo no encontrado: " + fileName;
        }
        System.out.println("Buscando cuenta de origen en: " + file.getAbsolutePath() + " a las " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
          br.readLine();
          br.readLine();
          String line;
          while ((line = br.readLine()) != null) {
            String[] tokens = line.split("\\|");
            if (tokens.length >= 3) {
              String id_count = tokens[0].trim();
              if (id_count.equals(idOrigen)) {
                String saldoStr = tokens[2].trim().replace(",", "");
                System.out.println("Saldo de origen (" + id_count + "): " + saldoStr + " a las " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
                saldoOrigen = Double.parseDouble(saldoStr);
                origenFile = fileName;
                break;
              }
            }
          }
        }
      } catch (NumberFormatException e) {
        logTransaction(idOrigen, idDestino, monto, "Pendiente");
        return "Error al parsear el saldo de la cuenta de origen: " + e.getMessage();
      } catch (IOException e) {
        logTransaction(idOrigen, idDestino, monto, "Pendiente");
        return "Error al leer el nodo " + nodeId + ": " + e.getMessage();
      }
      if (origenFile != null) break;
    }
    if (origenFile == null) {
      logTransaction(idOrigen, idDestino, monto, "Pendiente");
      return "Cuenta de origen no encontrada";
    }

    for (String fileName : table_counts) {
      try {
        File file = new File(root_directory + fileName);
        if (!file.exists()) {
          logTransaction(idOrigen, idDestino, monto, "Pendiente");
          return "Archivo no encontrado: " + fileName;
        }
        System.out.println("Buscando cuenta de destino en: " + file.getAbsolutePath() + " a las " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
          br.readLine();
          br.readLine();
          String line;
          while ((line = br.readLine()) != null) {
            String[] tokens = line.split("\\|");
            if (tokens.length >= 3) {
              String id_count = tokens[0].trim();
              if (id_count.equals(idDestino)) {
                String saldoStr = tokens[2].trim().replace(",", "");
                System.out.println("Saldo de destino (" + id_count + "): " + saldoStr + " a las " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
                saldoDestino = Double.parseDouble(saldoStr);
                destinoFile = fileName;
                break;
              }
            }
          }
        }
      } catch (NumberFormatException e) {
        logTransaction(idOrigen, idDestino, monto, "Pendiente");
        return "Error al parsear el saldo de la cuenta de destino: " + e.getMessage();
      } catch (IOException e) {
        logTransaction(idOrigen, idDestino, monto, "Pendiente");
        return "Error al leer el nodo " + nodeId + ": " + e.getMessage();
      }
      if (destinoFile != null) break;
    }
    if (destinoFile == null) {
      logTransaction(idOrigen, idDestino, monto, "Pendiente");
      return "Cuenta de destino no encontrada";
    }

    if (saldoOrigen < monto) {
      logTransaction(idOrigen, idDestino, monto, "Pendiente");
      return "Saldo insuficiente en la cuenta de origen";
    }

    saldoOrigen -= monto;
    saldoDestino += monto;

    synchronized (fileLock) {
      List<String> lines = new ArrayList<>();
      try (BufferedReader br = new BufferedReader(new FileReader(root_directory + origenFile))) {
        String line;
        while ((line = br.readLine()) != null) {
          lines.add(line);
        }
      } catch (IOException e) {
        logTransaction(idOrigen, idDestino, monto, "Pendiente");
        return "Error al leer el archivo de origen: " + e.getMessage();
      }

      try (BufferedWriter bw = new BufferedWriter(new FileWriter(root_directory + origenFile))) {
        bw.write(lines.get(0) + "\n");
        bw.write(lines.get(1) + "\n");
        for (int i = 2; i < lines.size(); i++) {
          String line = lines.get(i);
          String[] tokens = line.split("\\|");
          if (tokens.length >= 3) {
            String id_count = tokens[0].trim();
            if (id_count.equals(idOrigen)) {
              bw.write(String.format("%s | %s | %.2f | %s\n", id_count, tokens[1].trim(), saldoOrigen, tokens[3].trim()));
            } else {
              bw.write(line + "\n");
            }
          }
        }
        System.out.println("Archivo de origen " + origenFile + " actualizado con saldo: " + saldoOrigen + " a las " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
      } catch (IOException e) {
        logTransaction(idOrigen, idDestino, monto, "Pendiente");
        return "Error al actualizar la cuenta de origen: " + e.getMessage();
      }

      // Enviar el contenido completo del archivo origen al servidor
      StringBuilder contenidoOrigen = new StringBuilder();
      try (BufferedReader br = new BufferedReader(new FileReader(root_directory + origenFile))) {
        String line;
        while ((line = br.readLine()) != null) {
          contenidoOrigen.append(line).append("\n");
        }
      } catch (IOException e) {
        System.err.println("Error al leer el contenido de " + origenFile + " para replicación: " + e.getMessage());
      }
      outToServer.println("UPDATE:" + nodeId + ":" + origenFile + ":" + contenidoOrigen.toString().replace("\n", "\\n"));
    }

    synchronized (fileLock) {
      List<String> lines = new ArrayList<>();
      try (BufferedReader br = new BufferedReader(new FileReader(root_directory + destinoFile))) {
        String line;
        while ((line = br.readLine()) != null) {
          lines.add(line);
        }
      } catch (IOException e) {
        logTransaction(idOrigen, idDestino, monto, "Pendiente");
        return "Error al leer el archivo de destino: " + e.getMessage();
      }

      try (BufferedWriter bw = new BufferedWriter(new FileWriter(root_directory + destinoFile))) {
        bw.write(lines.get(0) + "\n");
        bw.write(lines.get(1) + "\n");
        for (int i = 2; i < lines.size(); i++) {
          String line = lines.get(i);
          String[] tokens = line.split("\\|");
          if (tokens.length >= 3) {
            String id_count = tokens[0].trim();
            if (id_count.equals(idDestino)) {
              bw.write(String.format("%s | %s | %.2f | %s\n", id_count, tokens[1].trim(), saldoDestino, tokens[3].trim()));
            } else {
              bw.write(line + "\n");
            }
          }
        }
        System.out.println("Archivo de destino " + destinoFile + " actualizado con saldo: " + saldoDestino + " a las " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
      } catch (IOException e) {
        logTransaction(idOrigen, idDestino, monto, "Pendiente");
        return "Error al actualizar la cuenta de destino: " + e.getMessage();
      }

      // Enviar el contenido completo del archivo destino al servidor
      StringBuilder contenidoDestino = new StringBuilder();
      try (BufferedReader br = new BufferedReader(new FileReader(root_directory + destinoFile))) {
        String line;
        while ((line = br.readLine()) != null) {
          contenidoDestino.append(line).append("\n");
        }
      } catch (IOException e) {
        System.err.println("Error al leer el contenido de " + destinoFile + " para replicación: " + e.getMessage());
      }
      outToServer.println("UPDATE:" + nodeId + ":" + destinoFile + ":" + contenidoDestino.toString().replace("\n", "\\n"));
    }

    logTransaction(idOrigen, idDestino, monto, "Confirmada");
    return "Transferencia exitosa en " + origenFile + " y " + destinoFile + ". Saldo origen: " + saldoOrigen + ", Saldo destino: " + saldoDestino;
  }

  private void handleUpdates() {
    try {
      while (true) {
        String update = inFromServer.readLine();
        if (update != null && update.startsWith("UPDATE:")) {
          System.out.println(nodeId + " recibió actualización: " + update + " a las " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
          String[] parts = update.split(":", 4);
          if (parts.length < 4) continue;
          String tabla = parts[2];
          String contenido = parts[3].replace("\\n", "\n");
          if (Arrays.asList(table_counts).contains(tabla)) {
            aplicarActualizacion(tabla, contenido);
          }
        }
      }
    } catch (IOException e) {
      System.err.println("Error al manejar actualizaciones en " + nodeId + ": " + e.getMessage());
    }
  }

  private void aplicarActualizacion(String tabla, String contenido) {
    synchronized (fileLock) {
      File file = new File(root_directory + tabla);
      try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
        bw.write(contenido);
        bw.flush();
        System.out.println(nodeId + " reemplazó el contenido de " + tabla + " a las " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
      } catch (IOException e) {
        System.err.println("Error al reemplazar el contenido de " + tabla + " en " + nodeId + ": " + e.getMessage());
      }
    }
  }

  public static void main(String[] args) {
    String root_directory = "/home/centos/nodes/node_1";
    new Thread(new Nodo("nodo_1", 7000, root_directory)).start();
  }
}