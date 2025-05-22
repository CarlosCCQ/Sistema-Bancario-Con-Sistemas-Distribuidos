import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class NodoTrabajador {
    private final int id;
    private final String ipServidor;
    private final int puertoServidor;
    private final String ipNodo;
    private final int puertoNodo;
    private final String rutaDatos;
    private final Map<Integer, Particion> particiones;
    private volatile boolean running;
    private ServerSocket serverSocket;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public NodoTrabajador(int id, String ipServidor, int puertoServidor, String ipNodo, int puertoNodo,
            String rutaDatos) {
        this.id = id;
        this.ipServidor = ipServidor;
        this.puertoServidor = puertoServidor;
        this.ipNodo = ipNodo;
        this.puertoNodo = puertoNodo;
        this.rutaDatos = rutaDatos;
        this.particiones = new ConcurrentHashMap<>();
        cargarParticiones();
    }

    private void cargarParticiones() {
        try {
            Files.createDirectories(Paths.get(rutaDatos));

            int[][] asignaciones;
            if (id == 1) {
                asignaciones = new int[][] { { 1, 1 }, { 1, 2 }, { 2, 3 }, { 3, 1 } };
            } else {
                asignaciones = new int[][] { { 1, 3 }, { 2, 1 }, { 2, 2 }, { 3, 2 }, { 3, 3 } };
            }

            for (int[] asignacion : asignaciones) {
                int particionId = asignacion[0];
                int replica = asignacion[1];
                String archivo = rutaDatos + "/particion_" + particionId + "_rep" + replica + ".dat";

                if (!particiones.containsKey(particionId)) {
                    particiones.put(particionId, new Particion(archivo));
                }
            }
        } catch (IOException e) {
            System.err.println("Error creando directorios: " + e.getMessage());
        }
    }

    private void sincronizarParticion(int particionId, String datos, PrintWriter out) {
        Particion p = particiones.get(particionId);
        if (p == null) {
            out.println("ERROR|PARTICION_NO_EXISTE");
            return;
        }

        lock.writeLock().lock();
        try {
            boolean exito = p.actualizarDesdeJson(datos);
            out.println(exito ? "OK" : "ERROR|SINCRONIZACION_FALLIDA");
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void sincronizarReplica(int particionId, Particion particion) {
        try {
            int otroNodo = (id == 1) ? 2 : 1;
            String ipOtro = "localhost";
            int puertoOtro = (otroNodo == 1) ? 6000 : 6001;

            String datos = particion.obtenerDatosJson();
            String mensaje = "SINCRONIZAR|" + particionId + "|" + datos;

            try (Socket socket = new Socket(ipOtro, puertoOtro);
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
                out.println(mensaje);
            }
        } catch (Exception e) {
            System.err.println("Error sincronizando con nodo " + ((id == 1) ? 2 : 1) + ": " + e.getMessage());
        }
    }

    private void verificarYRepararReplicas() {
        while (running) {
            try {
                Thread.sleep(30000);
                int otroNodo = (id == 1) ? 2 : 1;
                String ipOtro = "localhost";
                int puertoOtro = (otroNodo == 1) ? 6000 : 6001;

                for (Integer particionId : particiones.keySet()) {
                    try (Socket socket = new Socket(ipOtro, puertoOtro);
                            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                        socket.setSoTimeout(5000);
                        out.println("OBTENER_PARTICION|" + particionId);
                        String respuesta = in.readLine();

                        if (respuesta != null && !respuesta.startsWith("ERROR")) {
                            Particion p = particiones.get(particionId);
                            String datosLocales = p.obtenerDatosJson();
                        }
                    } catch (Exception e) {
                        // Handle other exceptions if needed
                    }
                }
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    public void iniciar() {
        running = true;
        new Thread(this::iniciarServidor).start();
        new Thread(this::gestionarConexionServidor).start();
        new Thread(this::verificarYRepararReplicas).start();
    }

    private void iniciarServidor() {
        try (ServerSocket serverSocket = new ServerSocket(puertoNodo)) {
            System.out.println("Nodo " + id + " escuchando en " + ipNodo + ":" + puertoNodo);
            while (running) {
                Socket socket = serverSocket.accept();
                new Thread(() -> procesarConexion(socket)).start();
            }
        } catch (IOException e) {
            System.err.println("Error iniciando servidor del nodo: " + e.getMessage());
        }
    }

    private void gestionarConexionServidor() {
        while (running) {
            try (Socket socket = new Socket(ipServidor, puertoServidor + 1);
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

                registrarEnServidor(out);
                while (running) {
                    Thread.sleep(5000);
                    enviarHeartbeat(out);
                }
            } catch (Exception e) {
                System.err.println("Reconectando en 5 segundos...");
                dormir(5000);
            }
        }
    }

    private void registrarEnServidor(PrintWriter out) {
        String particionesStr = particiones.keySet().stream()
                .map(p -> "CUENTA:" + p)
                .reduce((a, b) -> a + "," + b).orElse("");
        String mensaje = String.format("REGISTRO|%d|%s|%d|%s", id, ipNodo, puertoNodo, particionesStr);
        out.println(mensaje);
    }

    private void enviarHeartbeat(PrintWriter out) {
        out.println("HEARTBEAT|" + id);
    }

    private void procesarConexion(Socket socket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            String mensaje;
            while ((mensaje = in.readLine()) != null) {
                String[] partes = mensaje.split("\\|");
                switch (partes[0]) {
                    case "CONSULTAR" -> procesarConsulta(partes[1], out);
                    case "TRANSFERIR" -> procesarTransferencia(partes[1], partes[2], partes[3], out);
                    case "ARQUEO" -> procesarArqueo(out);
                    case "HEARTBEAT" -> out.println("OK");
                    case "OBTENER_PARTICION" -> obtenerParticion(Integer.parseInt(partes[1]), out);
                    case "ACTUALIZAR_PARTICION" -> actualizarParticion(Integer.parseInt(partes[1]), partes[2], out);
                    case "SINCRONIZAR" -> sincronizarParticion(Integer.parseInt(partes[1]), partes[2], out);
                    case "BLOQUEAR_ARQUEO" -> out.println("OK");
                    case "DESBLOQUEAR_ARQUEO" -> out.println("OK");
                }
            }
        } catch (IOException e) {
            System.err.println("Error en conexión: " + e.getMessage());
        }
    }

    private void procesarConsulta(String idCuentaStr, PrintWriter out) {
        int idCuenta = Integer.parseInt(idCuentaStr);
        int particion = hashParticion(idCuenta);
        Particion p = particiones.get(particion);

        if (p == null) {
            out.println("ERROR|PARTICION_NO_LOCAL");
            return;
        }
        Cuenta cuenta = p.getCuenta(idCuenta);
        out.println(cuenta != null ? "SALDO|" + cuenta.getSaldo() : "ERROR|CUENTA_NO_EXISTE");
    }

    private void procesarTransferencia(String origenStr, String destinoStr, String montoStr, PrintWriter out) {
        int origen = Integer.parseInt(origenStr);
        int destino = Integer.parseInt(destinoStr);
        double monto = Double.parseDouble(montoStr);
        int particion = hashParticion(origen);
        Particion p = particiones.get(particion);

        if (p == null) {
            out.println("ERROR|PARTICION_NO_LOCAL");
            return;
        }

        lock.writeLock().lock();
        try {
            boolean exito = p.transferir(origen, destino, monto);
            if (exito) {
                sincronizarReplica(particion, p);
            }
            out.println(exito ? "OK" : "ERROR|SALDO_INSUFICIENTE");
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void procesarArqueo(PrintWriter out) {
        lock.readLock().lock();
        try {
            double total = particiones.values().stream()
                    .mapToDouble(Particion::arqueoLocal)
                    .sum();
            out.printf("ARQUEO|%.2f%n", total);
        } finally {
            lock.readLock().unlock();
        }
    }

    private void obtenerParticion(int particionId, PrintWriter out) {
        Particion p = particiones.get(particionId);
        if (p == null) {
            out.println("ERROR|PARTICION_NO_EXISTE");
            return;
        }

        lock.readLock().lock();
        try {
            String datos = p.obtenerDatosJson();
            out.println(datos);
        } finally {
            lock.readLock().unlock();
        }
    }

    private void actualizarParticion(int particionId, String datos, PrintWriter out) {
        Particion p = particiones.get(particionId);
        if (p == null) {
            out.println("ERROR|PARTICION_NO_EXISTE");
            return;
        }

        lock.writeLock().lock();
        try {
            boolean exito = p.actualizarDesdeJson(datos);
            out.println(exito ? "OK" : "ERROR|ACTUALIZACION_FALLIDA");
        } finally {
            lock.writeLock().unlock();
        }
    }

    private int hashParticion(int id) {
        return (Math.abs(id) % 3) + 1;
    }

    private static void dormir(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) {
        if (args.length != 6) {
            System.err.println(
                    "Uso: NodoTrabajador <id> <ipServidor> <puertoServidor> <ipNodo> <puertoNodo> <rutaDatos>");
            return;
        }
        new NodoTrabajador(
                Integer.parseInt(args[0]),
                args[1],
                Integer.parseInt(args[2]),
                args[3],
                Integer.parseInt(args[4]),
                args[5]).iniciar();
    }
}