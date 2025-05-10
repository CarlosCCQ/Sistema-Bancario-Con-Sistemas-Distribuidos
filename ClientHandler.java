import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ClientHandler extends Thread {
    private Socket clienteSocket;
    
    public ClientHandler(Socket socket){
        this.clienteSocket = socket;
    }

    @Override
    public void run(){
        try(BufferedReader in = new BufferedReader(new InputStreamReader(clienteSocket.getInputStream()));
            PrintWriter out = new PrintWriter(clienteSocket.getOutputStream(), true)){
            String mensaje = in.readLine();
            String respuesta = ServidorCentral.reenviarANodoDisponible(mensaje);
            out.println(respuesta);
        } catch (IOException e){
            System.err.println("Error al manejar la conexión del cliente: "+ e.getMessage());
        }
    }
}
