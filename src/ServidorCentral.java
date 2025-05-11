package src;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
//import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
//import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
//import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ScheduledExecutorService;
import java.util.Collections;
import java.util.Comparator;

public class ServidorCentral {
    private ServerSocket serverSocket;
    private final int port;
    private volatile boolean running;
    private final Map<Integer, NodoHandler> nodos = new ConcurrentHashMap<>();
    private final Map<String, List<Integer>> replicas = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService heartbeatScheduler = Executors.newScheduledThreadPool(1);
    private final LoadBalancer loadBalancer = new LoadBalancer();

    public ServidorCentral(int port) {
        this.port = port;
    }

    public void iniciar() throws IOException {
        serverSocket = new ServerSocket(port);
        running = true;
        new Thread(this::aceptarConexionesNodos).start();
        iniciarHeartbeat();

        while (running) {
            Socket clienteSocket = serverSocket.accept();
            executor.execute(() -> manejarCliente(clienteSocket));
        }
    }

    private void aceptarConexionesNodos() {
        try (ServerSocket nodoServerSocket = new ServerSocket(port + 1)) {
            while (running) {
                Socket nodoSocket = nodoServerSocket.accept();
                procesarRegistroNodo(nodoSocket);
            }
        } catch (IOException e) {
            if (running) System.err.println("Error en conexión nodos: " + e.getMessage());
        }
    }

    private void procesarRegistroNodo(Socket socket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
            
            String registro = in.readLine();
            String[] partes = registro.split("\\|");
            int nodoId = Integer.parseInt(partes[1]);
            String ipNodo = partes[2];
            int puertoNodo = Integer.parseInt(partes[3]);

            Arrays.stream(partes[4].split(";")).forEach(t -> {
                String[] datos = t.split(":");
                String tabla = datos[0];
                Arrays.stream(datos[1].split(","))
                    .map(Integer::parseInt)
                    .forEach(p -> {
                        String key = tabla + "_" + p;
                        replicas.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>()).add(nodoId);
                        loadBalancer.addNodo(key, nodoId);
                    });
            });

            NodoHandler nuevoNodo = new NodoHandler(nodoId, ipNodo, puertoNodo);
            nodos.put(nodoId, nuevoNodo);
            loadBalancer.actualizarMetricas(nodoId, nuevoNodo);
            out.println("REGISTRO_EXITOSO");
        } catch (Exception e) {
            System.err.println("Error registrando nodo: " + e.getMessage());
        }
    }

    private void iniciarHeartbeat() {
        heartbeatScheduler.scheduleAtFixedRate(() -> {
            nodos.forEach((id, nodo) -> {
                try {
                    nodo.enviarYRecibir("HEARTBEAT", 2);
                    nodo.setActivo(true);
                    nodo.actualizarMetricas();
                } catch (TimeoutException e) {
                    nodo.setActivo(false);
                }
            });
        }, 0, 10, TimeUnit.SECONDS);
    }

    private void manejarCliente(Socket socket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
            
            String mensaje;
            while ((mensaje = in.readLine()) != null && !mensaje.isEmpty()) {
                String[] partes = mensaje.split("\\|", 4);
                switch (partes[0]) {
                    case "CONSULTAR_SALDO" -> consultarSaldo(partes[1], out);
                    case "TRANSFERIR_FONDOS" -> transferirFondos(partes[1], partes[2], partes[3], out);
                    case "ARQUEO" -> arqueoGlobal(out);
                    default -> out.println("ERROR|OPERACION_NO_SOPORTADA");
                }
            }
        } catch (IOException e) {
            System.err.println("Error con cliente: " + e.getMessage());
        }
    }

    private void consultarSaldo(String idCuenta, PrintWriter out) {
        int particion = hashParticion(idCuenta);
        List<Integer> nodosReplica = replicas.get("CUENTA_" + particion);
        
        if (nodosReplica == null || nodosReplica.isEmpty()) {
            out.println("ERROR|PARTICION_NO_ENCONTRADA");
            return;
        }

        try {
            Integer mejorNodo = loadBalancer.seleccionarNodo("CUENTA_" + particion);
            String respuesta = nodos.get(mejorNodo).enviarYRecibir("CONSULTAR|" + idCuenta, 5);
            out.println(respuesta);
        } catch (TimeoutException | NullPointerException e) {
            out.println("ERROR|NODOS_NO_DISPONIBLES");
        }
    }

    private void transferirFondos(String origen, String destino, String monto, PrintWriter out) {
        int particionOrigen = hashParticion(origen);
        List<Integer> nodosOrigen = replicas.get("CUENTA_" + particionOrigen);
        
        if (nodosOrigen == null || nodosOrigen.isEmpty()) {
            out.println("ERROR|PARTICION_NO_ENCONTRADA");
            return;
        }

        AtomicInteger exitos = new AtomicInteger(0);
        List<CompletableFuture<Void>> futures = nodosOrigen.stream()
            .map(nodoId -> CompletableFuture.runAsync(() -> procesarTransferencia(nodoId, origen, destino, monto, exitos), executor))
            .toList();

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .get(15, TimeUnit.SECONDS);

            if (exitos.get() >= (nodosOrigen.size() / 2 + 1)) {
                out.println("OK|TRANSFERENCIA_EXITOSA");
                repararReplicasFallidas(nodosOrigen, particionOrigen);
            } else {
                out.println("ERROR|CONSISTENCIA_NO_GARANTIZADA");
            }
        } catch (TimeoutException e) {
            out.println("ERROR|TIEMPO_EXCEDIDO");
        } catch (Exception e) {
            out.println("ERROR|ERROR_INTERNO");
        }
    }

    private void procesarTransferencia(int nodoId, String origen, String destino, String monto, AtomicInteger exitos) {
        NodoHandler nodo = nodos.get(nodoId);
        if (nodo == null || !nodo.estaActivo()) return;

        try {
            String respuesta = nodo.enviarYRecibir("TRANSFERIR|" + origen + "|" + destino + "|" + monto, 10);
            if ("OK".equals(respuesta)) exitos.incrementAndGet();
        } catch (TimeoutException e) {
            nodo.setActivo(false);
        }
    }

    private void arqueoGlobal(PrintWriter out) {
        try {
            bloquearOperaciones();
            double total = calcularTotalGlobal();
            out.printf("ARQUEO|%.2f%n", total);
        } catch (TimeoutException e) {
            out.println("ERROR|ARQUEO_FALLIDO");
        } finally {
            desbloquearOperaciones();
        }
    }

    private double calcularTotalGlobal() throws TimeoutException {
        return replicas.keySet().parallelStream()
            .filter(k -> k.startsWith("CUENTA_"))
            .mapToDouble(k -> {
                Integer nodoId = loadBalancer.seleccionarNodo(k);
                try {
                    return Double.parseDouble(nodos.get(nodoId).enviarYRecibir("ARQUEO", 10));
                } catch (TimeoutException e) {
                    return 0.0;
                }
            }).sum();
    }

    private void bloquearOperaciones() throws TimeoutException {
        for (String key : replicas.keySet()) {
            Integer nodoId = loadBalancer.seleccionarNodo(key);
            nodos.get(nodoId).enviarYRecibir("BLOQUEAR_ARQUEO", 5);
        }
    }

    private void desbloquearOperaciones() {
        replicas.keySet().forEach(key -> {
            Integer nodoId = loadBalancer.seleccionarNodo(key);
            try {
                nodos.get(nodoId).enviarYRecibir("DESBLOQUEAR_ARQUEO", 5);
            } catch (TimeoutException e) {
                System.err.println("Error desbloqueando nodo " + nodoId);
            }
        });
    }

    private int hashParticion(String id) {
        try {
            return (Math.abs(Integer.parseInt(id)) % 3) + 1;
        } catch (NumberFormatException e) {
            return 1;
        }
    }

    private void repararReplicasFallidas(List<Integer> nodosReplica, int particion) {
        nodosReplica.parallelStream()
            .filter(nodoId -> !nodos.get(nodoId).estaActivo())
            .forEach(nodoId -> resincronizarNodo(nodoId, particion));
    }

    private void resincronizarNodo(int nodoId, int particion) {
        List<Integer> nodosSanos = replicas.get("CUENTA_" + particion).stream()
            .filter(id -> id != nodoId && nodos.get(id).estaActivo())
            .toList();

        if (!nodosSanos.isEmpty()) {
            try {
                NodoHandler fuente = nodos.get(nodosSanos.get(0));
                String datos = fuente.enviarYRecibir("OBTENER_PARTICION|" + particion, 30);
                nodos.get(nodoId).enviarYRecibir("ACTUALIZAR_PARTICION|" + particion + "|" + datos, 30);
                nodos.get(nodoId).setActivo(true);
            } catch (TimeoutException e) {
                System.err.println("Fallo resincronización nodo " + nodoId);
            }
        }
    }

    private class NodoHandler {
        private final int id;
        private final String ip;
        private final int puerto;
        private volatile boolean activo;
        private final AtomicInteger carga = new AtomicInteger(0);
        private final AtomicLong ultimaRespuesta = new AtomicLong(System.currentTimeMillis());

        public NodoHandler(int id, String ip, int puerto) {
            this.id = id;
            this.ip = ip;
            this.puerto = puerto;
            this.activo = true;
        }

        public String enviarYRecibir(String mensaje, int timeout) throws TimeoutException {
            carga.incrementAndGet();
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(ip, puerto), timeout * 1000);
                socket.setSoTimeout(timeout * 1000);

                PrintWriter out = new PrintWriter(
                    new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), 
                    true
                );
                
                BufferedReader in = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8)
                );

                out.println(mensaje);
                String respuesta = in.readLine();
                ultimaRespuesta.set(System.currentTimeMillis());
                return respuesta;
            } catch (SocketTimeoutException e) {
                activo = false;
                throw new TimeoutException();
            } catch (IOException e) {
                activo = false;
                throw new TimeoutException();
            } finally {
                carga.decrementAndGet();
            }
        }

        public void actualizarMetricas() {
        }

        public boolean estaActivo() {
            return activo;
        }

        public void setActivo(boolean activo) {
            this.activo = activo;
        }
    }

    private class LoadBalancer {
        private final Map<String, List<Integer>> nodosPorParticion = new ConcurrentHashMap<>();
        private final Map<Integer, NodoHandler> metricasNodos = new ConcurrentHashMap<>();

        public void addNodo(String particion, int nodoId) {
            nodosPorParticion.computeIfAbsent(particion, k -> new CopyOnWriteArrayList<>()).add(nodoId);
        }

        public Integer seleccionarNodo(String particion) {
            return nodosPorParticion.getOrDefault(particion, Collections.emptyList()).stream()
                .filter(nodoId -> metricasNodos.get(nodoId).estaActivo())
                .min(Comparator.comparingInt(n -> metricasNodos.get(n).carga.get()))
                .orElse(null);
        }

        public void actualizarMetricas(int nodoId, NodoHandler handler) {
            metricasNodos.put(nodoId, handler);
        }
    }

    public static void main(String[] args) throws IOException {
        new ServidorCentral(5000).iniciar();
    }
}
