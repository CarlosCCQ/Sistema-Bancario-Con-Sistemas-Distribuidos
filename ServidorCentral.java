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
    private final Map<Integer, Long> ultimoHeartbeat = new ConcurrentHashMap<>();

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
                executor.execute(() -> procesarRegistroNodo(nodoSocket));
            }
        } catch (IOException e) {
            if (running)
                System.err.println("Error en conexión nodos: " + e.getMessage());
        }
    }

    private void procesarRegistroNodo(Socket socket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            String linea;
            while ((linea = in.readLine()) != null) {
                String[] partes = linea.split("\\|");

                if ("REGISTRO".equals(partes[0])) {
                    int nodoId = Integer.parseInt(partes[1]);
                    String ipNodo = partes[2];
                    int puertoNodo = Integer.parseInt(partes[3]);

                    Arrays.stream(partes[4].split(",")).forEach(t -> {
                        String[] datos = t.split(":");
                        String tabla = datos[0];
                        int particion = Integer.parseInt(datos[1]);
                        String key = tabla + "_" + particion;
                        replicas.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>()).add(nodoId);
                        loadBalancer.addNodo(key, nodoId);
                    });

                    NodoHandler nuevoNodo = new NodoHandler(nodoId, ipNodo, puertoNodo);
                    nodos.put(nodoId, nuevoNodo);
                    loadBalancer.actualizarMetricas(nodoId, nuevoNodo);
                    ultimoHeartbeat.put(nodoId, System.currentTimeMillis());
                    out.println("REGISTRO_EXITOSO");
                } else if ("HEARTBEAT".equals(partes[0])) {
                    int nodoId = Integer.parseInt(partes[1]);
                    ultimoHeartbeat.put(nodoId, System.currentTimeMillis());
                    NodoHandler nodo = nodos.get(nodoId);
                    if (nodo != null) {
                        nodo.setActivo(true);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error procesando nodo: " + e.getMessage());
        }
    }

    private void iniciarHeartbeat() {
        heartbeatScheduler.scheduleAtFixedRate(() -> {
            long tiempoActual = System.currentTimeMillis();
            ultimoHeartbeat.forEach((nodoId, ultimoTiempo) -> {
                NodoHandler nodo = nodos.get(nodoId);
                if (nodo != null) {
                    if ((tiempoActual - ultimoTiempo) > 15000) {
                        nodo.setActivo(false);
                        System.err.println("Nodo " + nodoId + " marcado como inactivo");
                        repararNodoFallido(nodoId);
                    }
                }
            });
        }, 0, 10, TimeUnit.SECONDS);
    }

    private void repararNodoFallido(int nodoFallido) {
        executor.execute(() -> {
            try {
                Thread.sleep(5000);

                replicas.forEach((particion, nodosLista) -> {
                    if (nodosLista.contains(nodoFallido)) {
                        List<Integer> nodosSanos = nodosLista.stream()
                                .filter(id -> id != nodoFallido && nodos.get(id).estaActivo())
                                .toList();

                        if (!nodosSanos.isEmpty()) {
                            try {
                                int particionId = Integer.parseInt(particion.split("_")[1]);
                                NodoHandler nodoSano = nodos.get(nodosSanos.get(0));
                                String datos = nodoSano.enviarYRecibir("OBTENER_PARTICION|" + particionId, 10);

                                Thread.sleep(10000);

                                NodoHandler nodoReparado = nodos.get(nodoFallido);
                                if (nodoReparado != null && nodoReparado.estaActivo()) {
                                    nodoReparado.enviarYRecibir("ACTUALIZAR_PARTICION|" + particionId + "|" + datos,
                                            15);
                                    System.out
                                            .println("Nodo " + nodoFallido + " reparado para partición " + particionId);
                                }
                            } catch (Exception e) {
                                System.err.println("Error reparando nodo " + nodoFallido + ": " + e.getMessage());
                            }
                        }
                    }
                });
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    private void manejarCliente(Socket socket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            String mensaje;
            while ((mensaje = in.readLine()) != null && !mensaje.isEmpty()) {
                String[] partes = mensaje.split("\\|", 4);
                switch (partes[0]) {
                    case "CONSULTAR_SALDO" -> consultarSaldoConFailover(partes[1], out);
                    case "TRANSFERIR_FONDOS" -> transferirFondosConConsistencia(partes[1], partes[2], partes[3], out);
                    case "ARQUEO" -> arqueoGlobal(out);
                    default -> out.println("ERROR|OPERACION_NO_SOPORTADA");
                }
            }
        } catch (IOException e) {
            System.err.println("Error con cliente: " + e.getMessage());
        }
    }

    private void consultarSaldoConFailover(String idCuenta, PrintWriter out) {
        int particion = hashParticion(idCuenta);
        List<Integer> nodosReplica = replicas.get("CUENTA_" + particion);

        if (nodosReplica == null || nodosReplica.isEmpty()) {
            out.println("ERROR|PARTICION_NO_ENCONTRADA");
            return;
        }

        for (Integer nodoId : nodosReplica) {
            NodoHandler nodo = nodos.get(nodoId);
            if (nodo != null && nodo.estaActivo()) {
                try {
                    String respuesta = nodo.enviarYRecibir("CONSULTAR|" + idCuenta, 5);
                    out.println(respuesta);
                    return;
                } catch (TimeoutException e) {
                    nodo.setActivo(false);
                    continue;
                }
            }
        }
        out.println("ERROR|TODOS_LOS_NODOS_INACTIVOS");
    }

    private void transferirFondosConConsistencia(String origen, String destino, String monto, PrintWriter out) {
        int particionOrigen = hashParticion(origen);
        List<Integer> nodosOrigen = replicas.get("CUENTA_" + particionOrigen);

        if (nodosOrigen == null || nodosOrigen.isEmpty()) {
            out.println("ERROR|PARTICION_NO_ENCONTRADA");
            return;
        }

        List<Integer> nodosActivos = nodosOrigen.stream()
                .filter(id -> nodos.get(id).estaActivo())
                .toList();

        if (nodosActivos.isEmpty()) {
            out.println("ERROR|NODOS_NO_DISPONIBLES");
            return;
        }

        AtomicInteger exitos = new AtomicInteger(0);
        AtomicInteger fallos = new AtomicInteger(0);
        List<CompletableFuture<Void>> futures = nodosActivos.stream()
                .map(nodoId -> CompletableFuture.runAsync(() -> {
                    try {
                        NodoHandler nodo = nodos.get(nodoId);
                        String respuesta = nodo.enviarYRecibir("TRANSFERIR|" + origen + "|" + destino + "|" + monto,
                                10);
                        if ("OK".equals(respuesta)) {
                            exitos.incrementAndGet();
                        } else {
                            fallos.incrementAndGet();
                        }
                    } catch (TimeoutException e) {
                        nodos.get(nodoId).setActivo(false);
                        fallos.incrementAndGet();
                    }
                }, executor))
                .toList();

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(20, TimeUnit.SECONDS);

            int totalRespuestas = exitos.get() + fallos.get();
            if (exitos.get() > 0 && exitos.get() >= (totalRespuestas / 2)) {
                out.println("OK|TRANSFERENCIA_EXITOSA");
            } else {
                out.println("ERROR|TRANSFERENCIA_FALLIDA");
            }
        } catch (TimeoutException e) {
            out.println("ERROR|TIEMPO_EXCEDIDO");
        } catch (Exception e) {
            out.println("ERROR|ERROR_INTERNO");
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
                    List<Integer> nodosParticion = replicas.get(k);
                    for (Integer nodoId : nodosParticion) {
                        NodoHandler nodo = nodos.get(nodoId);
                        if (nodo != null && nodo.estaActivo()) {
                            try {
                                String respuesta = nodo.enviarYRecibir("ARQUEO", 10);
                                return Double.parseDouble(respuesta.split("\\|")[1]);
                            } catch (Exception e) {
                                continue;
                            }
                        }
                    }
                    return 0.0;
                }).sum();
    }

    private void bloquearOperaciones() throws TimeoutException {
        for (String key : replicas.keySet()) {
            List<Integer> nodosParticion = replicas.get(key);
            for (Integer nodoId : nodosParticion) {
                NodoHandler nodo = nodos.get(nodoId);
                if (nodo != null && nodo.estaActivo()) {
                    try {
                        nodo.enviarYRecibir("BLOQUEAR_ARQUEO", 5);
                        break;
                    } catch (TimeoutException e) {
                        continue;
                    }
                }
            }
        }
    }

    private void desbloquearOperaciones() {
        replicas.keySet().forEach(key -> {
            List<Integer> nodosParticion = replicas.get(key);
            for (Integer nodoId : nodosParticion) {
                NodoHandler nodo = nodos.get(nodoId);
                if (nodo != null && nodo.estaActivo()) {
                    try {
                        nodo.enviarYRecibir("DESBLOQUEAR_ARQUEO", 5);
                        break;
                    } catch (TimeoutException e) {
                        continue;
                    }
                }
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
                        true);

                BufferedReader in = new BufferedReader(
                        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

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