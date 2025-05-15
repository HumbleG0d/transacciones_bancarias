package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"strconv"
)

type Nodo struct {
	nodeId        string
	portNode      int
	serverHost    string
	serverPort    int
	serverConn    net.Conn
	listener      net.Listener
	rootDirectory string
	tableCounts   []string
	fileLock      sync.Mutex
	transactionLock sync.Mutex
	transactionId int
}

func NewNodo(nodeId string, portNode int, rootDirectory string) *Nodo {
	n := &Nodo{
		nodeId:        nodeId,
		portNode:      portNode,
		serverHost:    "127.0.0.1",
		serverPort:    5000,
		rootDirectory: rootDirectory,
		tableCounts:   []string{"cu_1.txt", "cu_2.txt", "cu_3.txt"},
	}
	if !strings.HasSuffix(rootDirectory, string(filepath.Separator)) {
		n.rootDirectory += string(filepath.Separator)
	}
	n.initializeTransactionFile()
	return n
}

func (n *Nodo) initializeTransactionFile() {
	transactionFile := filepath.Join(n.rootDirectory, "transacciones.txt")
	if _, err := os.Stat(transactionFile); os.IsNotExist(err) {
		file, err := os.Create(transactionFile)
		if err != nil {
			fmt.Printf("Error al crear transacciones.txt: %v\n", err)
			return
		}
		defer file.Close()
		writer := bufio.NewWriter(file)
		writer.WriteString("ID_TRANSACC | ID_ORIG | ID_DEST | MONTO  | FECHA_HORA         | ESTADO\n")
		writer.WriteString("----------------------------------------------------------------------------\n")
		writer.Flush()
		now := time.Now().Format("15:04:05")
		fmt.Printf("Archivo transacciones.txt creado con encabezado a las %s\n", now)
	} else {
		file, err := os.Open(transactionFile)
		if err != nil {
			fmt.Printf("Error al leer transacciones.txt: %v\n", err)
			return
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		scanner.Scan() // Saltar encabezado
		scanner.Scan() // Saltar línea divisoria
		for scanner.Scan() {
			line := scanner.Text()
			tokens := strings.Split(line, "|")
			if len(tokens) >= 1 {
				if id, err := strconv.Atoi(strings.TrimSpace(tokens[0])); err == nil {
					n.transactionId = id
				} else {
					fmt.Printf("Error al parsear ID_TRANSACC en transacciones.txt: %s\n", tokens[0])
				}
			}
		}
		now := time.Now().Format("15:04:05")
		fmt.Printf("Archivo transacciones.txt existente. Último ID_TRANSACC leído: %d a las %s\n", n.transactionId, now)
	}
}

func (n *Nodo) Run() {
	err := n.connectionServer()
	if err != nil {
		fmt.Printf("Error al crear el nodo %s: %v\n", n.nodeId, err)
		return
	}
	n.registerNode()
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", n.portNode))
	if err != nil {
		fmt.Printf("Error al crear el nodo %s: %v\n", n.nodeId, err)
		return
	}
	n.listener = listener
	now := time.Now().Format("15:04:05")
	fmt.Printf("Nodo %s iniciado en el puerto %d a las %s\n", n.nodeId, n.portNode, now)

	go n.handleTasks()
	go n.handleUpdates()

	select {} // Mantener el programa corriendo
}

func (n *Nodo) connectionServer() error {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", n.serverHost, n.serverPort))
	if err != nil {
		return err
	}
	n.serverConn = conn
	return nil
}

func (n *Nodo) registerNode() error {
	ip := getLocalIP() // Obtener la IP del nodo
	tables := strings.Join(n.tableCounts, ",")
	msg := fmt.Sprintf("REGISTRO_NODO:%s:%s:%d:%s", n.nodeId, ip, n.portNode, tables)
	fmt.Fprintf(n.serverConn, "%s\n", msg)
	now := time.Now().Format("15:04:05")
	fmt.Printf("Nodo %s registrado en el servidor con IP %s y tablas: %s a las %s\n", n.nodeId, ip, tables, now)
	return nil
}

func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "127.0.0.1"
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return "127.0.0.1"
}

func (n *Nodo) handleTasks() {
	for {
		conn, err := n.listener.Accept()
		if err != nil {
			fmt.Printf("Error al aceptar tareas en %s: %v\n", n.nodeId, err)
			continue
		}
		now := time.Now().Format("15:04:05")
		fmt.Printf("Nodo %s recibió una tarea de %s a las %s\n", n.nodeId, conn.RemoteAddr(), now)
		go n.handleTask(conn)
	}
}

func (n *Nodo) handleTask(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	option, err := reader.ReadString('\n')
	if err != nil {
		fmt.Printf("Error al leer tarea en %s: %v\n", n.nodeId, err)
		return
	}
	option = strings.TrimSpace(option)

	if strings.HasPrefix(option, "1") {
		idCount := strings.Split(option, "-")[1]
		result := n.checkBalance(idCount)
		writer.WriteString(fmt.Sprintf("RESULTADO %s -> %s\n", n.nodeId, result))
	} else if strings.HasPrefix(option, "2") {
		tokens := strings.Split(option, "-")
		details := strings.Split(tokens[1], ":")
		idOrigen := details[0]
		idDestino := details[1]
		montoStr := details[2]
		result := n.transferBalance(idOrigen, idDestino, montoStr)
		writer.WriteString(fmt.Sprintf("RESULTADO %s -> TRANSFERENCIA DE %s A %s POR %s - %s\n", n.nodeId, idOrigen, idDestino, montoStr, result))
	}
	writer.Flush()
}

func (n *Nodo) checkBalance(idClient string) string {
	for _, fileName := range n.tableCounts {
		filePath := filepath.Join(n.rootDirectory, fileName)
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			return fmt.Sprintf("Archivo no encontrado: %s", fileName)
		}
		now := time.Now().Format("15:04:05")
		fmt.Printf("Leyendo archivo para checkBalance: %s a las %s\n", filePath, now)
		file, err := os.Open(filePath)
		if err != nil {
			return fmt.Sprintf("Error al leer el nodo %s: %v", n.nodeId, err)
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		scanner.Scan() // Saltar encabezado
		scanner.Scan() // Saltar línea divisoria
		for scanner.Scan() {
			line := scanner.Text()
			tokens := strings.Split(line, "|")
			if len(tokens) >= 3 {
				idCount := strings.TrimSpace(tokens[0])
				if idCount == idClient {
					saldoStr := strings.TrimSpace(strings.ReplaceAll(tokens[2], ",", ""))
					return fmt.Sprintf("SALDO: %s", saldoStr)
				}
			}
		}
	}
	return "CUENTA NO ENCONTRADA"
}

func (n *Nodo) logTransaction(idOrigen, idDestino string, monto float64, estado string) {
	n.transactionLock.Lock()
	defer n.transactionLock.Unlock()
	n.transactionId++
	now := time.Now().Format("2006-01-02 15:04:05")
	line := fmt.Sprintf("%-10d | %-7s | %-7s | %-7.2f | %-19s | %s\n",
		n.transactionId, idOrigen, idDestino, monto, now, estado)
	transactionFile := filepath.Join(n.rootDirectory, "transacciones.txt")
	file, err := os.OpenFile(transactionFile, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Error al registrar la transacción: %v a las %s\n", err, time.Now().Format("15:04:05"))
		return
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	writer.WriteString(line)
	writer.Flush()
	nowStr := time.Now().Format("15:04:05")
	fmt.Printf("Transacción registrada y escrita: ID_TRANSACC=%d, ID_ORIG=%s, ID_DEST=%s, MONTO=%.2f, FECHA_HORA=%s, ESTADO=%s a las %s\n",
		n.transactionId, idOrigen, idDestino, monto, now, estado, nowStr)
}

func (n *Nodo) transferBalance(idOrigen, idDestino, amount string) string {
	now := time.Now().Format("15:04:05")
	fmt.Printf("TRANSFERENCIA: %s %s %s a las %s\n", idOrigen, idDestino, amount, now)
	monto, err := strconv.ParseFloat(amount, 64)
	if err != nil {
		n.logTransaction(idOrigen, idDestino, 0.0, "Pendiente")
		return fmt.Sprintf("Error al parsear el monto: %v", err)
	}

	var origenFile, destinoFile string
	var saldoOrigen, saldoDestino float64

	for _, fileName := range n.tableCounts {
		filePath := filepath.Join(n.rootDirectory, fileName)
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			n.logTransaction(idOrigen, idDestino, monto, "Pendiente")
			return fmt.Sprintf("Archivo no encontrado: %s", fileName)
		}
		now := time.Now().Format("15:04:05")
		fmt.Printf("Buscando cuenta de origen en: %s a las %s\n", filePath, now)
		file, err := os.Open(filePath)
		if err != nil {
			n.logTransaction(idOrigen, idDestino, monto, "Pendiente")
			return fmt.Sprintf("Error al leer el nodo %s: %v", n.nodeId, err)
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		scanner.Scan() // Saltar encabezado
		scanner.Scan() // Saltar línea divisoria
		for scanner.Scan() {
			line := scanner.Text()
			tokens := strings.Split(line, "|")
			if len(tokens) >= 3 {
				idCount := strings.TrimSpace(tokens[0])
				if idCount == idOrigen {
					saldoStr := strings.TrimSpace(strings.ReplaceAll(tokens[2], ",", ""))
					saldoOrigen, err = strconv.ParseFloat(saldoStr, 64)
					if err != nil {
						n.logTransaction(idOrigen, idDestino, monto, "Pendiente")
						return fmt.Sprintf("Error al parsear el saldo de la cuenta de origen: %v", err)
					}
					fmt.Printf("Saldo de origen (%s): %f a las %s\n", idCount, saldoOrigen, now)
					origenFile = fileName
					break
				}
			}
		}
		if origenFile != "" {
			break
		}
	}
	if origenFile == "" {
		n.logTransaction(idOrigen, idDestino, monto, "Pendiente")
		return "Cuenta de origen no encontrada"
	}

	for _, fileName := range n.tableCounts {
		filePath := filepath.Join(n.rootDirectory, fileName)
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			n.logTransaction(idOrigen, idDestino, monto, "Pendiente")
			return fmt.Sprintf("Archivo no encontrado: %s", fileName)
		}
		now := time.Now().Format("15:04:05")
		fmt.Printf("Buscando cuenta de destino en: %s a las %s\n", filePath, now)
		file, err := os.Open(filePath)
		if err != nil {
			n.logTransaction(idOrigen, idDestino, monto, "Pendiente")
			return fmt.Sprintf("Error al leer el nodo %s: %v", n.nodeId, err)
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		scanner.Scan() // Saltar encabezado
		scanner.Scan() // Saltar línea divisoria
		for scanner.Scan() {
			line := scanner.Text()
			tokens := strings.Split(line, "|")
			if len(tokens) >= 3 {
				idCount := strings.TrimSpace(tokens[0])
				if idCount == idDestino {
					saldoStr := strings.TrimSpace(strings.ReplaceAll(tokens[2], ",", ""))
					saldoDestino, err = strconv.ParseFloat(saldoStr, 64)
					if err != nil {
						n.logTransaction(idOrigen, idDestino, monto, "Pendiente")
						return fmt.Sprintf("Error al parsear el saldo de la cuenta de destino: %v", err)
					}
					fmt.Printf("Saldo de destino (%s): %f a las %s\n", idCount, saldoDestino, now)
					destinoFile = fileName
					break
				}
			}
		}
		if destinoFile != "" {
			break
		}
	}
	if destinoFile == "" {
		n.logTransaction(idOrigen, idDestino, monto, "Pendiente")
		return "Cuenta de destino no encontrada"
	}

	if saldoOrigen < monto {
		n.logTransaction(idOrigen, idDestino, monto, "Pendiente")
		return "Saldo insuficiente en la cuenta de origen"
	}

	saldoOrigen -= monto
	saldoDestino += monto

	n.fileLock.Lock()
	defer n.fileLock.Unlock()

	// Actualizar archivo de origen
	lines, err := readFileLines(filepath.Join(n.rootDirectory, origenFile))
	if err != nil {
		n.logTransaction(idOrigen, idDestino, monto, "Pendiente")
		return fmt.Sprintf("Error al leer el archivo de origen: %v", err)
	}
	err = writeFileLines(filepath.Join(n.rootDirectory, origenFile), updateLines(lines, idOrigen, saldoOrigen))
	if err != nil {
		n.logTransaction(idOrigen, idDestino, monto, "Pendiente")
		return fmt.Sprintf("Error al actualizar la cuenta de origen: %v", err)
	}
	now = time.Now().Format("15:04:05")
	fmt.Printf("Archivo de origen %s actualizado con saldo: %.2f a las %s\n", origenFile, saldoOrigen, now)

	// Enviar actualización del archivo origen
	content, err := readFileContent(filepath.Join(n.rootDirectory, origenFile))
	if err == nil {
		fmt.Fprintf(n.serverConn, "UPDATE:%s:%s:%s\n", n.nodeId, origenFile, strings.ReplaceAll(content, "\n", "\\n"))
	}

	// Actualizar archivo de destino
	lines, err = readFileLines(filepath.Join(n.rootDirectory, destinoFile))
	if err != nil {
		n.logTransaction(idOrigen, idDestino, monto, "Pendiente")
		return fmt.Sprintf("Error al leer el archivo de destino: %v", err)
	}
	err = writeFileLines(filepath.Join(n.rootDirectory, destinoFile), updateLines(lines, idDestino, saldoDestino))
	if err != nil {
		n.logTransaction(idOrigen, idDestino, monto, "Pendiente")
		return fmt.Sprintf("Error al actualizar la cuenta de destino: %v", err)
	}
	now = time.Now().Format("15:04:05")
	fmt.Printf("Archivo de destino %s actualizado con saldo: %.2f a las %s\n", destinoFile, saldoDestino, now)

	// Enviar actualización del archivo destino
	content, err = readFileContent(filepath.Join(n.rootDirectory, destinoFile))
	if err == nil {
		fmt.Fprintf(n.serverConn, "UPDATE:%s:%s:%s\n", n.nodeId, destinoFile, strings.ReplaceAll(content, "\n", "\\n"))
	}

	n.logTransaction(idOrigen, idDestino, monto, "Confirmada")
	return fmt.Sprintf("Transferencia exitosa en %s y %s. Saldo origen: %.2f, Saldo destino: %.2f", origenFile, destinoFile, saldoOrigen, saldoDestino)
}

func readFileLines(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

func writeFileLines(filePath string, lines []string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	for _, line := range lines {
		writer.WriteString(line + "\n")
	}
	return writer.Flush()
}

func updateLines(lines []string, id string, saldo float64) []string {
	for i := 2; i < len(lines); i++ {
		tokens := strings.Split(lines[i], "|")
		if len(tokens) >= 3 {
			idCount := strings.TrimSpace(tokens[0])
			if idCount == id {
				lines[i] = fmt.Sprintf("%s | %s | %.2f | %s", idCount, strings.TrimSpace(tokens[1]), saldo, strings.TrimSpace(tokens[3]))
			}
		}
	}
	return lines
}

func readFileContent(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	var content strings.Builder
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		content.WriteString(scanner.Text() + "\n")
	}
	return content.String(), scanner.Err()
}

func (n *Nodo) handleUpdates() {
	reader := bufio.NewReader(n.serverConn)
	for {
		update, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("Error al manejar actualizaciones en %s: %v\n", n.nodeId, err)
			return
		}
		update = strings.TrimSpace(update)
		if strings.HasPrefix(update, "UPDATE:") {
			now := time.Now().Format("15:04:05")
			fmt.Printf("%s recibió actualización: %s a las %s\n", n.nodeId, update, now)
			parts := strings.SplitN(update, ":", 4)
			if len(parts) >= 4 {
				tabla := parts[2]
				contenido := strings.ReplaceAll(parts[3], "\\n", "\n")
				if contains(n.tableCounts, tabla) {
					n.aplicarActualizacion(tabla, contenido)
				}
			}
		}
	}
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func (n *Nodo) aplicarActualizacion(tabla, contenido string) {
	n.fileLock.Lock()
	defer n.fileLock.Unlock()
	filePath := filepath.Join(n.rootDirectory, tabla)
	file, err := os.Create(filePath)
	if err != nil {
		fmt.Printf("Error al reemplazar el contenido de %s en %s: %v\n", tabla, n.nodeId, err)
		return
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	writer.WriteString(contenido)
	writer.Flush()
	now := time.Now().Format("15:04:05")
	fmt.Printf("%s reemplazó el contenido de %s a las %s\n", n.nodeId, tabla, now)
}

func main() {
	rootDirectory := "C:\\Users\\sergi\\transacciones_bancarias\\parcial\\src\\main\\java\\com\\examen\\nodes\\nodo_1\\"
	n := NewNodo("nodo_1", 6001, rootDirectory)
	go n.Run()
	time.Sleep(time.Hour * 24) // Mantener el programa vivo (ajustar según necesidad)
}