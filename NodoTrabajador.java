import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NodoTrabajador {
    private static final Map<Integer, Cuenta> cuentas = Collections.synchronizedMap(new HashMap<>());
    private static final List<Transaccion> transacciones = Collections.synchronizedList(new ArrayList<>());
    private static final List<String> otrosNodos = new ArrayList<>();
    private static int transaccionCounter = 0;

    public static void main(String[] args) throws IOException{
        int puerto = args.length > 0 ? Integer.parseInt(args[0]) : 5001;
        ServerSocket serverSocket = new ServerSocket(puerto, 50, InetAddress.getByName("0.0.0.0"));
        System.out.println("Nodo trabajador iniciado en el puerto " + puerto);
        
        cargarCuentasDesdeArchivo("cuentas_ " + puerto + ".txt");
        cargarOtrosNodos(puerto);
        
        while(true){
            Socket socket = serverSocket.accept();
            new Thread(() -> manejarCliente(socket)).start();
        }
    }

    private static void cargarOtrosNodos(int puertoPropio){
        int[] puertos = {5001, 5002, 5003};
        for(int puerto : puertos){
            if (puerto != puertoPropio){
                otrosNodos.add("localhost:" + puerto);
            }
        }
    }

    private static void replicar(String mensaje){
        for(String nodo: otrosNodos){
            String[] datos = nodo.split(",");
            try (Socket socket = new Socket(datos[0], Integer.parseInt(datos[1]));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)){
                    out.println("SYNC," + mensaje);
            } catch (IOException e){
                System.err.println("Error al sincronizar con nodo" + nodo);
            }
        }
    }

    private static void cargarCuentasDesdeArchivo(String archivo){
        try(BufferedReader reader = new BufferedReader(new FileReader(archivo))){
            String linea;
            while((linea = reader.readLine()) != null){
                String[] partes = linea.split(",");
                int id = Integer.parseInt(partes[0]);
                int clienteId = Integer.parseInt(partes[1]);
                double saldo = Double.parseDouble(partes[2]);
                String tipo = partes[3];
                cuentas.put(id, new Cuenta(id, clienteId, saldo, tipo));
            }
        } catch (IOException e){
            System.err.println("Error al cargar cuentas: " + e.getMessage());
        }
    }

    private static void manejarCliente(Socket socket){
        try(BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true)){
                String linea = in.readLine();
                String[] partes = linea.split(",");

                boolean esSync = partes[0].equals("SYNC");
                String tipo = esSync ? partes[1] : partes[0];

                if(tipo.equals("CONSULTAR")){
                    int id = Integer.parseInt(partes[1]);
                    Cuenta cuenta = cuentas.get(id);
                    out.println(cuenta != null ? "Saldo: " + cuenta.getSaldo() : "Cuenta no encontrada");
                } else if(tipo.equals("TRANSFERIR")){
                    int origen = Integer.parseInt(partes[esSync ? 2 : 1]);
                    int destino = Integer.parseInt(partes[esSync ? 3 : 2]);
                    double monto = Double.parseDouble(partes[esSync ? 4 : 3]);

                    Cuenta cuentaOrigen = cuentas.get(origen);
                    Cuenta cuentaDestino = cuentas.get(destino);

                    if(cuentaOrigen != null && cuentaDestino != null && cuentaOrigen.retirar(monto)){
                        cuentaDestino.depositar(monto);
                        String fechaHora = java.time.LocalDateTime.now().toString();
                        Transaccion transaccion = new Transaccion(transaccionCounter++, origen, destino, monto, fechaHora, esSync ? "Confirmada (sync)" : "Confirmada");
                        transacciones.add(transaccion);
                        if(!esSync){
                            replicar("TRANSFERIR," + origen + "," + destino + "," + monto);
                            replicar("TRANSACCION," + transaccion.toString());
                        }
                        out.println("Transferencia exitosa: "+ transaccion.toString());
                    }else {
                        out.println("Transferencia fallida");
                    }
                } else if(tipo.equals("TRANSACCION")){
                    Transaccion transaccion = Transaccion.fromString(String.join(",", Arrays.copyOfRange(partes, esSync ? 2 : 1, partes.length)));
                    transacciones.add(transaccion);
                    out.println("Transacción sincronizada: " + transaccion.id);
                }
            } catch (IOException e){
                e.printStackTrace();
            }
    }
}