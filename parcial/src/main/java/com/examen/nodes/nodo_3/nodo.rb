require 'socket'

class Nodo
  attr_reader :node_id, :port_node, :server_host, :server_port, :root_directory, :table_counts

  def initialize(node_id, port_node, root_directory)
    @node_id = node_id
    @port_node = port_node
    @server_host = "127.0.0.1"
    @server_port = 5000
    @root_directory = root_directory
    @table_counts = ["cu_1.txt", "cu_2.txt", "cu_3.txt"]
  end

  def run
    begin
      # Conexión al servidor central
      socket = TCPSocket.new(server_host, server_port)
      out_to_server = socket
      in_from_server = socket

      # Registro del nodo
      out_to_server.puts("REGISTRO_NODO#{node_id}:#{port_node}")

      # Inicializar el nodo para escuchar tareas
      server = TCPServer.new(port_node)
      puts "Nodo #{node_id} iniciado en el puerto #{port_node}"

      loop do
        socket_task = server.accept
        Thread.new(socket_task) do |task|
          begin
            handle_task(task)
          ensure
            task.close
          end
        end
      end
    rescue StandardError => e
      $stderr.puts "Error al crear el nodo #{node_id}: #{e.message}"
    end
  end

  def handle_task(socket)
    reader = socket
    writer = socket

    option = reader.gets&.chomp
    return unless option

    if option.start_with?("1")
      # Simular tarea
      parts = option.split("-")
      if parts.length < 2
        writer.puts("RESULTADO #{node_id} -> Formato inválido. Usa: 1-ID_CUENTA")
        return
      end
      id_count = parts[1].strip
      result = check_balance(id_count)
      writer.puts("RESULTADO #{node_id} -> #{result}")
    end
  rescue StandardError => e
    $stderr.puts "Error al manejar la tarea en el nodo #{node_id}: #{e.message}"
  end

  def check_balance(id_client)
    table_counts.each_with_index do |file_name, x|
      begin
        file_path = File.join(root_directory, file_name)
        File.open(file_path) do |file|
          br = file
          # Saltar encabezado y línea divisoria
          br.gets
          br.gets

          br.each_line do |line|
            tokens = line.split("|")
            id_count = tokens[0].strip
            if id_count == id_client
              return "SALDO: #{tokens[2].strip}"
            end
          end
        end
      rescue StandardError => e
        return "Error al leer el nodo #{node_id}: #{e.message}"
      end
    end
    "CUENTA NO ENCONTRADA"
  end

  if __FILE__ == $PROGRAM_NAME
    root_directory = "src/main/java/com/examen/nodes/nodo_3"
    nodo = Nodo.new("nodo_1", 6003, root_directory)
    nodo.run
  end
end