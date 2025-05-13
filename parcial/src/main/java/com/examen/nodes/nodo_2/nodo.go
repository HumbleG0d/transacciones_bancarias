package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
)

type Nodo struct {
	nodeId        string
	portNode      int
	serverHost    string
	serverPort    int
	rootDirectory string
	tableCounts   []string
}

func NewNodo(nodeId string, portNode int, rootDirectory string) *Nodo {
	return &Nodo{
		nodeId:        nodeId,
		portNode:      portNode,
		serverHost:    "127.0.0.1",
		serverPort:    5000,
		rootDirectory: rootDirectory,
		tableCounts:   []string{"cu_1.txt", "cu_2.txt", "cu_3.txt"},
	}
}

func (n *Nodo) Run() {
	// Conexión al servidor central
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", n.serverHost, n.serverPort))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error al crear el nodo %s: %v\n", n.nodeId, err)
		return
	}
	defer conn.Close()

	outToServer := bufio.NewWriter(conn)
	inFromServer := bufio.NewReader(conn)

	// Registro del nodo
	_, err = outToServer.WriteString("REGISTRO_NODO" + n.nodeId + ":" + fmt.Sprintf("%d", n.portNode) + "\n")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error al registrar el nodo %s: %v\n", n.nodeId, err)
		return
	}
	outToServer.Flush()

	// Inicializar el nodo para escuchar tareas
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", n.portNode))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error al crear el nodo %s: %v\n", n.nodeId, err)
		return
	}
	defer listener.Close()

	fmt.Printf("Nodo %s iniciado en el puerto %d\n", n.nodeId, n.portNode)

	var wg sync.WaitGroup
	for {
		connTask, err := listener.Accept()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error al aceptar tarea en el nodo %s: %v\n", n.nodeId, err)
			continue
		}
		wg.Add(1)
		go func(socket net.Conn) {
			defer wg.Done()
			n.handleTask(socket)
			socket.Close()
		}(connTask)
	}
	wg.Wait()
}

func (n *Nodo) handleTask(conn net.Conn) {
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	option, err := reader.ReadString('\n')
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error al manejar la tarea en el nodo %s: %v\n", n.nodeId, err)
		return
	}
	option = strings.TrimSpace(option)

	if strings.HasPrefix(option, "1") {
		// Simular tarea
		parts := strings.Split(option, "-")
		if len(parts) < 2 {
			writer.WriteString("RESULTADO " + n.nodeId + " -> Formato inválido. Usa: 1-ID_CUENTA\n")
			writer.Flush()
			return
		}
		idCount := strings.TrimSpace(parts[1])
		result := n.checkBalance(idCount)
		writer.WriteString("RESULTADO " + n.nodeId + " -> " + result + "\n")
		writer.Flush()
	}
}

func (n *Nodo) checkBalance(idClient string) string {
	x := 0
	for x < len(n.tableCounts) {
		filePath := n.rootDirectory + n.tableCounts[x]
		file, err := os.Open(filePath)
		if err != nil {
			return "Error al leer el nodo " + n.nodeId + ": " + err.Error()
		}
		defer file.Close()

		br := bufio.NewReader(file)
		// Saltar encabezado y línea divisoria
		br.ReadString('\n')
		br.ReadString('\n')

		for {
			line, err := br.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					break
				}
				return "Error al leer el nodo " + n.nodeId + ": " + err.Error()
			}
			tokens := strings.Split(line, "|")
			idCount := strings.TrimSpace(tokens[0])
			if idCount == idClient {
				return "SALDO: " + strings.TrimSpace(tokens[2])
			}
		}
		x++
	}
	return "CUENTA NO ENCONTRADA"
}

func main() {
	rootDirectory := "src/main/java/com/examen/nodes/nodo_2"
	nodo := NewNodo("nodo_1", 6001, rootDirectory)
	nodo.Run()
}