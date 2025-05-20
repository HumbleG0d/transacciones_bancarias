#!/usr/bin/env ruby
require 'socket'
require 'thread'
require 'fileutils'
require 'time'

class Nodo
  attr_reader :node_id, :port_node, :root_directory, :table_counts

  def initialize(node_id, port_node, root_directory)
    @node_id = node_id
    @port_node = port_node
    @server_host = ' ' # Colocar el IP del servidor
    @server_port = 5000
    @root_directory = root_directory.end_with?(File::SEPARATOR) ? root_directory : "#{root_directory}#{File::SEPARATOR}"
    @table_counts = ['cu_1.txt', 'cu_2.txt', 'cu_3.txt']
    @file_lock = Mutex.new
    @transaction_lock = Mutex.new
    @transaction_id = 0
    initialize_transaction_file
  end

  def initialize_transaction_file
    transaction_file = File.join(@root_directory, 'transacciones.txt')
    
    if !File.exist?(transaction_file)
      begin
        File.open(transaction_file, 'w') do |file|
          file.puts "ID_TRANSACC | ID_ORIG | ID_DEST | MONTO  | FECHA_HORA         | ESTADO"
          file.puts "----------------------------------------------------------------------------"
        end
        now = Time.now.strftime("%H:%M:%S")
        puts "Archivo transacciones.txt creado con encabezado a las #{now}"
      rescue => e
        puts "Error al crear transacciones.txt: #{e.message}"
      end
    else
      begin
        File.open(transaction_file, 'r') do |file|
          file.readline  # Saltar encabezado
          file.readline  # Saltar línea divisoria
          
          file.each_line do |line|
            tokens = line.split('|')
            if tokens.length >= 1
              begin
                @transaction_id = tokens[0].strip.to_i
              rescue => e
                puts "Error al parsear ID_TRANSACC en transacciones.txt: #{tokens[0].strip}"
              end
            end
          end
        end
        now = Time.now.strftime("%H:%M:%S")
        puts "Archivo transacciones.txt existente. Último ID_TRANSACC leído: #{@transaction_id} a las #{now}"
      rescue => e
        puts "Error al leer transacciones.txt: #{e.message}"
      end
    end
  end

  def run
    begin
      connection_server
      register_node
      @listener = TCPServer.new(@port_node)
      now = Time.now.strftime("%H:%M:%S")
      puts "Nodo #{@node_id} iniciado en el puerto #{@port_node} a las #{now}"

      Thread.new { handle_tasks }
      Thread.new { handle_updates }

      # Mantener el programa en ejecución
      loop do
        sleep 60
      end
    rescue => e
      puts "Error al crear el nodo #{@node_id}: #{e.message}"
    end
  end

  def connection_server
    @server_conn = TCPSocket.new(@server_host, @server_port)
  end

  def register_node
    ip = get_local_ip
    tables = @table_counts.join(',')
    msg = "REGISTRO_NODO:#{@node_id}:#{ip}:#{@port_node}:#{tables}"
    @server_conn.puts(msg)
    now = Time.now.strftime("%H:%M:%S")
    puts "Nodo #{@node_id} registrado en el servidor con IP #{ip} y tablas: #{tables} a las #{now}"
  end

  def get_local_ip
    # Intenta obtener la IP local
    begin
      # Crea un socket UDPSocket temporal
      socket = UDPSocket.new
      socket.connect("8.8.8.8", 1)  # No se envía ningún dato
      socket.addr.last
    rescue
      "127.0.0.1"
    ensure
      socket.close if socket
    end
  end

  def handle_tasks
    loop do
      begin
        client = @listener.accept
        client_addr = client.peeraddr[3]
        now = Time.now.strftime("%H:%M:%S")
        puts "Nodo #{@node_id} recibió una tarea de #{client_addr} a las #{now}"
        
        Thread.new { handle_task(client) }
      rescue => e
        puts "Error al aceptar tareas en #{@node_id}: #{e.message}"
      end
    end
  end

  def handle_task(client)
    begin
      option = client.gets.strip
      
      if option.start_with?('1')
        id_count = option.split('-')[1]
        result = check_balance(id_count)
        client.puts("RESULTADO #{@node_id} -> #{result}")
      elsif option.start_with?('2')
        tokens = option.split('-')
        details = tokens[1].split(':')
        id_origen = details[0]
        id_destino = details[1]
        monto_str = details[2]
        result = transfer_balance(id_origen, id_destino, monto_str)
        client.puts("RESULTADO #{@node_id} -> TRANSFERENCIA DE #{id_origen} A #{id_destino} POR #{monto_str} - #{result}")
      end
    rescue => e
      puts "Error al manejar la tarea en #{@node_id}: #{e.message}"
    ensure
      client.close
    end
  end

  def check_balance(id_client)
    @table_counts.each do |file_name|
      begin
        file_path = File.join(@root_directory, file_name)
        unless File.exist?(file_path)
          return "Archivo no encontrado: #{file_name}"
        end
        
        now = Time.now.strftime("%H:%M:%S")
        puts "Leyendo archivo para checkBalance: #{file_path} a las #{now}"
        
        File.open(file_path, 'r') do |file|
          file.readline  # Saltar encabezado
          file.readline  # Saltar línea divisoria
          
          file.each_line do |line|
            tokens = line.split('|')
            if tokens.length >= 3
              id_count = tokens[0].strip
              if id_count == id_client
                saldo_str = tokens[2].strip.gsub(',', '')
                return "SALDO: #{saldo_str}"
              end
            end
          end
        end
      rescue => e
        return "Error al leer el nodo #{@node_id}: #{e.message}"
      end
    end
    return "CUENTA NO ENCONTRADA"
  end

  def log_transaction(id_origen, id_destino, monto, estado)
    @transaction_lock.synchronize do
      @transaction_id += 1
      now = Time.now
      fecha_hora = now.strftime("%Y-%m-%d %H:%M:%S")
      
      begin
        transaction_file = File.join(@root_directory, 'transacciones.txt')
        File.open(transaction_file, 'a') do |file|
          line = sprintf("%-10d | %-7s | %-7s | %-7.2f | %-19s | %s\n", 
                         @transaction_id, id_origen, id_destino, monto, fecha_hora, estado)
          file.write(line)
        end
        now_str = Time.now.strftime("%H:%M:%S")
        puts "Transacción registrada y escrita: ID_TRANSACC=#{@transaction_id}, ID_ORIG=#{id_origen}, " +
             "ID_DEST=#{id_destino}, MONTO=#{monto}, FECHA_HORA=#{fecha_hora}, ESTADO=#{estado} a las #{now_str}"
      rescue => e
        now_str = Time.now.strftime("%H:%M:%S")
        puts "Error al registrar la transacción: #{e.message} a las #{now_str}"
      end
    end
  end

  def transfer_balance(id_origen, id_destino, amount_str)
    now = Time.now.strftime("%H:%M:%S")
    puts "TRANSFERENCIA: #{id_origen} #{id_destino} #{amount_str} a las #{now}"
    
    begin
      monto = Float(amount_str)
    rescue => e
      log_transaction(id_origen, id_destino, 0.0, "Pendiente")
      return "Error al parsear el monto: #{e.message}"
    end

    origen_file = nil
    destino_file = nil
    saldo_origen = 0.0
    saldo_destino = 0.0

    # Buscar cuenta de origen
    @table_counts.each do |file_name|
      begin
        file_path = File.join(@root_directory, file_name)
        unless File.exist?(file_path)
          log_transaction(id_origen, id_destino, monto, "Pendiente")
          return "Archivo no encontrado: #{file_name}"
        end
        
        now = Time.now.strftime("%H:%M:%S")
        puts "Buscando cuenta de origen en: #{file_path} a las #{now}"
        
        File.open(file_path, 'r') do |file|
          file.readline  # Saltar encabezado
          file.readline  # Saltar línea divisoria
          
          file.each_line do |line|
            tokens = line.split('|')
            if tokens.length >= 3
              id_count = tokens[0].strip
              if id_count == id_origen
                saldo_str = tokens[2].strip.gsub(',', '')
                saldo_origen = Float(saldo_str)
                origen_file = file_name
                puts "Saldo de origen (#{id_count}): #{saldo_origen} a las #{now}"
                break
              end
            end
          end
        end
        
        break if origen_file
      rescue => e
        log_transaction(id_origen, id_destino, monto, "Pendiente")
        return "Error al leer el nodo #{@node_id}: #{e.message}"
      end
    end

    if origen_file.nil?
      log_transaction(id_origen, id_destino, monto, "Pendiente")
      return "Cuenta de origen no encontrada"
    end

    # Buscar cuenta de destino
    @table_counts.each do |file_name|
      begin
        file_path = File.join(@root_directory, file_name)
        unless File.exist?(file_path)
          log_transaction(id_origen, id_destino, monto, "Pendiente")
          return "Archivo no encontrado: #{file_name}"
        end
        
        now = Time.now.strftime("%H:%M:%S")
        puts "Buscando cuenta de destino en: #{file_path} a las #{now}"
        
        File.open(file_path, 'r') do |file|
          file.readline  # Saltar encabezado
          file.readline  # Saltar línea divisoria
          
          file.each_line do |line|
            tokens = line.split('|')
            if tokens.length >= 3
              id_count = tokens[0].strip
              if id_count == id_destino
                saldo_str = tokens[2].strip.gsub(',', '')
                saldo_destino = Float(saldo_str)
                destino_file = file_name
                puts "Saldo de destino (#{id_count}): #{saldo_destino} a las #{now}"
                break
              end
            end
          end
        end
        
        break if destino_file
      rescue => e
        log_transaction(id_origen, id_destino, monto, "Pendiente")
        return "Error al leer el nodo #{@node_id}: #{e.message}"
      end
    end

    if destino_file.nil?
      log_transaction(id_origen, id_destino, monto, "Pendiente")
      return "Cuenta de destino no encontrada"
    end

    if saldo_origen < monto
      log_transaction(id_origen, id_destino, monto, "Pendiente")
      return "Saldo insuficiente en la cuenta de origen"
    end

    saldo_origen -= monto
    saldo_destino += monto

    @file_lock.synchronize do
      # Actualizar archivo de origen
      begin
        origen_path = File.join(@root_directory, origen_file)
        lines = File.readlines(origen_path)
        
        File.open(origen_path, 'w') do |file|
          file.write(lines[0])
          file.write(lines[1])
          
          lines[2..-1].each do |line|
            tokens = line.split('|')
            if tokens.length >= 3
              id_count = tokens[0].strip
              if id_count == id_origen
                file.puts("#{id_count} | #{tokens[1].strip} | #{sprintf('%.2f', saldo_origen)} | #{tokens[3].strip}")
              else
                file.write(line)
              end
            end
          end
        end
        
        now = Time.now.strftime("%H:%M:%S")
        puts "Archivo de origen #{origen_file} actualizado con saldo: #{sprintf('%.2f', saldo_origen)} a las #{now}"
        
        # Enviar actualización del archivo origen
        content = File.read(origen_path)
        @server_conn.puts("UPDATE:#{@node_id}:#{origen_file}:#{content.gsub("\n", "\\n")}")
      rescue => e
        log_transaction(id_origen, id_destino, monto, "Pendiente")
        return "Error al actualizar la cuenta de origen: #{e.message}"
      end

      # Actualizar archivo de destino
      begin
        destino_path = File.join(@root_directory, destino_file)
        lines = File.readlines(destino_path)
        
        File.open(destino_path, 'w') do |file|
          file.write(lines[0])
          file.write(lines[1])
          
          lines[2..-1].each do |line|
            tokens = line.split('|')
            if tokens.length >= 3
              id_count = tokens[0].strip
              if id_count == id_destino
                file.puts("#{id_count} | #{tokens[1].strip} | #{sprintf('%.2f', saldo_destino)} | #{tokens[3].strip}")
              else
                file.write(line)
              end
            end
          end
        end
        
        now = Time.now.strftime("%H:%M:%S")
        puts "Archivo de destino #{destino_file} actualizado con saldo: #{sprintf('%.2f', saldo_destino)} a las #{now}"
        
        # Enviar actualización del archivo destino
        content = File.read(destino_path)
        @server_conn.puts("UPDATE:#{@node_id}:#{destino_file}:#{content.gsub("\n", "\\n")}")
      rescue => e
        log_transaction(id_origen, id_destino, monto, "Pendiente")
        return "Error al actualizar la cuenta de destino: #{e.message}"
      end
    end

    log_transaction(id_origen, id_destino, monto, "Confirmada")
    return "Transferencia exitosa en #{origen_file} y #{destino_file}. Saldo origen: #{sprintf('%.2f', saldo_origen)}, Saldo destino: #{sprintf('%.2f', saldo_destino)}"
  end

  def handle_updates
    begin
      loop do
        update = @server_conn.gets.strip
        if update.start_with?('UPDATE:')
          now = Time.now.strftime("%H:%M:%S")
          puts "#{@node_id} recibió actualización: #{update} a las #{now}"
          
          parts = update.split(':', 4)
          if parts.length >= 4
            tabla = parts[2]
            contenido = parts[3].gsub('\\n', "\n")
            
            if @table_counts.include?(tabla)
              aplicar_actualizacion(tabla, contenido)
            end
          end
        end
      end
    rescue => e
      puts "Error al manejar actualizaciones en #{@node_id}: #{e.message}"
    end
  end

  def aplicar_actualizacion(tabla, contenido)
    @file_lock.synchronize do
      begin
        file_path = File.join(@root_directory, tabla)
        File.write(file_path, contenido)
        now = Time.now.strftime("%H:%M:%S")
        puts "#{@node_id} reemplazó el contenido de #{tabla} a las #{now}"
      rescue => e
        puts "Error al reemplazar el contenido de #{tabla} en #{@node_id}: #{e.message}"
      end
    end
  end
end

# Punto de entrada principal
if __FILE__ == $PROGRAM_NAME
  # Configuración del nodo - ajustar según necesidad
  root_directory = " " # Colocar la ruta de los directorios
  node = Nodo.new("nodo_ruby",  , root_directory) # Colocar el puerto del nodo
  node.run
end
