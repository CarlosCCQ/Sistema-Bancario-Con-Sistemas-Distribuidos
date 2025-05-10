import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

public class ClienteBanco {
    public static void main(String[] args) throws IOException{
        Scanner scanner = new Scanner(System.in);

        System.out.println("1. Consultar saldo\n2. Transferir fondos");
        int opcion = scanner.nextInt();
        scanner.nextLine();

        String mensaje = "";

        if(opcion == 1){
            System.out.println("Ingrese el ID de la cuenta:");
            int id = scanner.nextInt();
            mensaje = "CONSULTAR," + id;
        } else if (opcion == 2){
            System.out.println("Cuenta origen: ");
            int origen = scanner.nextInt();
            System.out.println("Cuenta destino: ");
            int destino = scanner.nextInt();
            System.out.println("Monto: ");
            double monto = scanner.nextDouble();
            mensaje = "TRANSFERENCIA, " + origen + "," + destino + "," + monto;
        }

        try (Socket socket = new Socket("localhost", 5000);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true)){
                out.println(mensaje);
                String respuesta = in.readLine();
                System.out.println("Respuesta  del servidor: " + respuesta);
            }
    }
}
