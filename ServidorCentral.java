import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

public class ServidorCentral {
    private static final String[][] nodos = {
        {"ip-NodoTrabajador1", "5001"},
        {"ip-NodoTrabajador2", "5002"},
        {"ip-NodoTrabajador3", "5003"}
    };

    private static int siguienteNodo = 0;

    public static void main(String[] args) throws IOException{
        ServerSocket serverSocket = new ServerSocket(5000);
        System.out.println("Servidor central escuchando en el puerto 5000...");

        while(true){
            Socket clienteSocket = serverSocket.accept();
            new ClientHandler(clienteSocket).start();
        }
    }

    public static synchronized String reenviarANodoDisponible(String mensaje){
        for(int i=0; i<nodos.length; i++){
            String ip = nodos[siguienteNodo][0];
            int puerto = Integer.parseInt(nodos[siguienteNodo][1]);
            siguienteNodo = (siguienteNodo + 1) % nodos.length;

            try(Socket nodoSocket = new Socket(ip, puerto);
                BufferedReader inNodo = new BufferedReader(new InputStreamReader(nodoSocket.getInputStream()));
                PrintWriter outNodo = new PrintWriter(nodoSocket.getOutputStream(), true)){
                    outNodo.println(mensaje);
                    return inNodo.readLine();
            } catch (IOException e){
                System.err.println("Nodo en " + ip + ":" + puerto + "no disponible. PRobando siguiente...");
            }
        }
        return "Error: Ningún nodo disponible.";
    }

    public static String[] getTodosLosNodos(){
        return Arrays.stream(nodos).map(n -> n[0] + ":" + n[1]).toArray(String[]::new);
    }
}
