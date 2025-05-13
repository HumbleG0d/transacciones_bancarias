package com.examen.nodes.nodo_1;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Nodo implements Runnable {

  private final String nodeId;
  private final int portNode;
  private final String serverHost = "127.0.0.1";
  private final int serverPort = 5000;
  private ServerSocket server;
  private Socket socket; //socket para conectarse con el servidor principal
  private BufferedReader inFromServer; //Para enviar mensajes del servidor
  private PrintWriter outToServer; //Para recibir mensajes del servidor
  private String root_directory;
  private String[] table_counts = {"cu_1.txt", "cu_2.txt", "cu_3.txt"};
  private final Object fileLock = new Object(); // Monitor para sincronización

  public Nodo(String nodeId, int portNode, String root_directory) {
    this.nodeId = nodeId;
    this.portNode = portNode;
    this.root_directory = root_directory.endsWith(File.separator) ? root_directory : root_directory + File.separator;
  }

  @Override
  public void run() {
    try {
      //Conexión del nodo al servidor central
      connectionServer();
      registerNode();

      //Notificar estatus del nodo

      //Inicial el nodo para escuchar tareas
      server = new ServerSocket(portNode);
      System.out.println("Nodo " + nodeId + " iniciado en el puerto " + portNode);

      while (true) {
        Socket socketTask = server.accept();
        System.out.println("Nodo " + nodeId + " recibió una tarea de " + socketTask.getInetAddress());
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

      if (option.startsWith("1")) {
        //Simular tarea
        String id_count = option.split("-")[1];
        String result = checkBalance(id_count);
        writer.println("RESULTADO " + nodeId + " -> " + result);
      } else if (option.startsWith("2")) {
        String[] tokens = option.split("-");
        String id_count = tokens[1].split(":")[0];
        String id_count_destino = tokens[1].split(":")[1];
        String monto = tokens[1].split(":")[2];
        transferBalance(id_count, id_count_destino, monto);
        writer.println("RESULTADO " + nodeId + " -> TRANSFERENCIA DE " + id_count + " A " + id_count_destino + " POR " + monto);
      }

    } catch (IOException e) {
      System.err.println("Error al manejar la tarea en el nodo " + nodeId + ": " + e.getMessage());
    }
  }

  private String checkBalance(String idClient) {
    int x = 0;
    while (x < table_counts.length) {
      String fileName = table_counts[x];
      try {
        File file = new File(root_directory + fileName);
        if (!Arrays.asList(table_counts).contains(fileName)) {
          return "Archivo no válido: " + fileName;
        }
        System.out.println("Leyendo archivo para checkBalance: " + file.getAbsolutePath());
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line;
        br.readLine();
        br.readLine();

        while ((line = br.readLine()) != null) {
          String[] tokens = line.split("\\|");
          if (tokens.length < 3) {
            br.close();
            return "Formato inválido en el archivo " + fileName;
          }
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

  //Transferencia de saldo entre cuentas
  private String transferBalance(String idOrigen, String idDestino, String amount) {
    System.out.println("TRANSFERENCIA: " + idOrigen + " " + idDestino + " " + amount);
    double monto = Double.parseDouble(amount);
    String origenFile = null;
    String destinoFile = null;
    double saldoOrigen = 0.0;
    double saldoDestino = 0.0;

    // Buscar cuenta de origen
    int x = 0;
    while (x < table_counts.length) {
      String fileName = table_counts[x];
      try {
        File file = new File(root_directory + fileName);
        if (!Arrays.asList(table_counts).contains(fileName)) {
          return "Archivo no válido: " + fileName;
        }
        System.out.println("Buscando cuenta de origen en: " + file.getAbsolutePath());
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line;
        br.readLine(); // Saltar encabezado
        br.readLine(); // Saltar línea divisoria

        while ((line = br.readLine()) != null) {
          String[] tokens = line.split("\\|");
          if (tokens.length < 4) {
            br.close();
            return "Formato inválido en el archivo " + fileName;
          }
          String id_count = tokens[0].trim();
          if (id_count.equals(idOrigen)) {
            String saldoStr = tokens[2].trim().replace(",", ""); // Eliminar comas
            System.out.println("Saldo de origen (" + id_count + "): " + saldoStr);
            saldoOrigen = Double.parseDouble(saldoStr);
            origenFile = table_counts[x];
            break;
          }
        }
        br.close();
      } catch (NumberFormatException e) {
        return "Error al parsear el saldo de la cuenta de origen: " + e.getMessage();
      } catch (IOException e) {
        return "Error al leer el nodo " + nodeId + ": " + e.getMessage();
      }
      x++;
    }
    if (origenFile == null) {
      return "Cuenta de origen no encontrada";
    }

    // Buscar cuenta de destino
    x = 0;
    while (x < table_counts.length) {
      String fileName = table_counts[x];
      try {
        File file = new File(root_directory + fileName);
        if (!Arrays.asList(table_counts).contains(fileName)) {
          return "Archivo no válido: " + fileName;
        }
        System.out.println("Buscando cuenta de destino en: " + file.getAbsolutePath());
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line;
        br.readLine(); // Saltar encabezado
        br.readLine(); // Saltar línea divisoria

        while ((line = br.readLine()) != null) {
          String[] tokens = line.split("\\|");
          if (tokens.length < 4) {
            br.close();
            return "Formato inválido en el archivo " + fileName;
          }
          String id_count = tokens[0].trim();
          if (id_count.equals(idDestino)) {
            String saldoStr = tokens[2].trim().replace(",", ""); // Eliminar comas
            System.out.println("Saldo de destino (" + id_count + "): " + saldoStr);
            saldoDestino = Double.parseDouble(saldoStr);
            destinoFile = table_counts[x];
            break;
          }
        }
        br.close();
      } catch (NumberFormatException e) {
        return "Error al parsear el saldo de la cuenta de destino: " + e.getMessage();
      } catch (IOException e) {
        return "Error al leer el nodo " + nodeId + ": " + e.getMessage();
      }
      x++;
    }
    if (destinoFile == null) {
      return "Cuenta de destino no encontrada";
    }

    // Verificar saldo suficiente
    if (saldoOrigen < monto) {
      return "Saldo insuficiente en la cuenta de origen";
    }

    // Realizar transferencia
    saldoOrigen -= monto;
    saldoDestino += monto;

    // Actualizar archivo de origen
    synchronized (fileLock) {
      List<String> lines = new ArrayList<>();
      try (BufferedReader br = new BufferedReader(new FileReader(root_directory + origenFile))) {
        String line;
        while ((line = br.readLine()) != null) {
          lines.add(line);
        }
      } catch (IOException e) {
        return "Error al leer el archivo de origen: " + e.getMessage();
      }

      try (BufferedWriter bw = new BufferedWriter(new FileWriter(root_directory + origenFile))) {
        String header = lines.size() > 0 ? lines.get(0) : "ID_CUENTA | ID_CLIENTE | SALDO | TIPO";
        String divider = lines.size() > 1 ? lines.get(1) : "--------------------------------------";
        System.out.println("Leyendo archivo de origen " + origenFile + ": Header=" + header + ", Divider=" + divider);
        bw.write(header + "\n");
        bw.write(divider + "\n");

        for (int i = 2; i < lines.size(); i++) {
          String line = lines.get(i);
          String[] tokens = line.split("\\|");
          if (tokens.length < 4) {
            return "Formato inválido en el archivo " + origenFile;
          }
          String id_count = tokens[0].trim();
          if (id_count.equals(idOrigen)) {
            bw.write(String.format("%s | %s | %.2f | %s\n", id_count, tokens[1].trim(), saldoOrigen, tokens[3].trim()));
          } else {
            bw.write(line + "\n");
          }
        }
        System.out.println("Archivo de origen " + origenFile + " actualizado con saldo: " + saldoOrigen);
      } catch (IOException e) {
        return "Error al actualizar la cuenta de origen: " + e.getMessage();
      }
    }

    // Actualizar archivo de destino
    synchronized (fileLock) {
      List<String> lines = new ArrayList<>();
      try (BufferedReader br = new BufferedReader(new FileReader(root_directory + destinoFile))) {
        String line;
        while ((line = br.readLine()) != null) {
          lines.add(line);
        }
      } catch (IOException e) {
        return "Error al leer el archivo de destino: " + e.getMessage();
      }

      try (BufferedWriter bw = new BufferedWriter(new FileWriter(root_directory + destinoFile))) {
        String header = lines.size() > 0 ? lines.get(0) : "ID_CUENTA | ID_CLIENTE | SALDO | TIPO";
        String divider = lines.size() > 1 ? lines.get(1) : "--------------------------------------";
        System.out.println("Leyendo archivo de destino " + destinoFile + ": Header=" + header + ", Divider=" + divider);
        bw.write(header + "\n");
        bw.write(divider + "\n");

        for (int i = 2; i < lines.size(); i++) {
          String line = lines.get(i);
          String[] tokens = line.split("\\|");
          if (tokens.length < 4) {
            return "Formato inválido en el archivo " + destinoFile;
          }
          String id_count = tokens[0].trim();
          if (id_count.equals(idDestino)) {
            bw.write(String.format("%s | %s | %.2f | %s\n", id_count, tokens[1].trim(), saldoDestino, tokens[3].trim()));
          } else {
            bw.write(line + "\n");
          }
        }
        System.out.println("Archivo de destino " + destinoFile + " actualizado con saldo: " + saldoDestino);
      } catch (IOException e) {
        return "Error al actualizar la cuenta de destino: " + e.getMessage();
      }
    }

    return "Transferencia exitosa. Saldo origen: " + saldoOrigen + ", Saldo destino: " + saldoDestino;
  }

  public static void main(String[] args) {
    String root_directory = "C:\\Users\\sergi\\transacciones_bancarias\\parcial\\src\\main\\java\\com\\examen\\nodes\\nodo_1\\";
    new Thread(new Nodo("nodo_1", 6000, root_directory)).start();
  }
}