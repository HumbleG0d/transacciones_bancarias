package com.examen.client;
import com.examen.test.TransactionDataGenerator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

public class Client  implements Runnable{

    private final String name;
    private final int PORT = 5000;
    private final String HOST = " " //Colocar la IP del seridor


    public Client (String name){
        this.name = name;
    }

    @Override
    public void run(){
        try(Scoket clienteSocket = new Socket(HOST , PORT); 
            PrintWriter out = new PrintWriter(clienteSocket.getOutputStream() , true);
            BufferedReader in = new BufferedReader(new InputStreamReader(clienteSocket.getInputStream()));)
            {
        
                Scanner sc = new Scanner(System.in);

                new Thread(() -> {
                    try{
          String serverMessage;
          while((serverMessage = in.readLine()) != null){
            System.out.println(serverMessage);
          }
        }catch (IOException e){
          System.err.println("Desconectado del servidor");
        }
                }).start();
        while(true){
            System.out.println("""
                OPCION 1 -> CONSULTAR SALDO 
                OPCION 2 -> REALIZAR UNA TRANASACCIOÓN
            """);

            System.out.print("Digite su opcion: ");
            String option = sc.nextLine();

            if(option.startsWith("1")){
                System.out.pritnln("CONSULTANDO SALDO .....");
                String id_count = sc.nextLine();
                out.println("1- " + id_count);
            }
            else if(option.startsWith("2")){
                System.out.println("REALIZANDO TRNASACCIONES ......");
                
                System.out.print("CUENTA ORIGEN: ");
                String id_count_origen = sc.nextLine();

                System.out.print("CUENTA DESTINO: ");
                String id_cout_destine = sc.nextLine();

                System.out.print("MONTO: ");
                String monto = sc.nextLine();

                out.println("2- " + id_count_origen + ":" + id_cout_destine + ":" + mont);
            }
            else{
                System.out.println("OPCION NO VALIDA! VUELVA A INTORDUCIR UNA OPCIÓN");
            }
        }

        }catch(IOException e){
            sout.err.println("ERROR: " + e.getMessage() )
        }
    }

    public start void main(String [] args){
        new Thread(new Client("SIDERAL")).start();
    }

}
