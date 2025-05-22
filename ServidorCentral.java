import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

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
        System.out.println("[Servidor] Iniciado en puerto " + port);
        new Thread(this::aceptarConexionesNodos).start();
        iniciarHeartbeat();

        while (running) {
            Socket clienteSocket = serverSocket.accept();
            System.out.println("[Servidor] Cliente conectado: " + clienteSocket.getInetAddress());
            executor.execute(() -> manejarCliente(clienteSocket));
        }
    }

    private void aceptarConexionesNodos() {
        try (ServerSocket nodoServerSocket = new ServerSocket(port + 1)) {
            System.out.println("[Servidor] Escuchando nodos en puerto " + (port + 1));
            while (running) {
                Socket nodoSocket = nodoServerSocket.accept();
                System.out.println("[Servidor] Nodo conectado: " + nodoSocket.getInetAddress());
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
                System.out.println("[Servidor] Mensaje nodo: " + linea);

                if (linea.contains("REGISTRO|") && linea.contains("HEARTBEAT|")) {
                    String[] mensajes = linea.split("HEARTBEAT\\|");
                    procesarMensajeNodo(mensajes[0], out);

                    for (int i = 1; i < mensajes.length; i++) {
                        if (!mensajes[i].isEmpty()) {
                            procesarMensajeNodo("HEARTBEAT|" + mensajes[i], out);
                        }
                    }
                } else {
                    procesarMensajeNodo(linea, out);
                }
            }
        } catch (Exception e) {
            System.err.println("Error procesando nodo: " + e.getMessage());
        }
    }

    private void procesarMensajeNodo(String mensaje, PrintWriter out) {
        String[] partes = mensaje.split("\\|");

        if ("REGISTRO".equals(partes[0]) && partes.length >= 5) {
            int nodoId = Integer.parseInt(partes[1]);
            String ipNodo = partes[2];
            int puertoNodo = Integer.parseInt(partes[3]);

            Arrays.stream(partes[4].split(",")).forEach(t -> {
                String[] datos = t.split(":");
                if (datos.length == 2) {
                    String tabla = datos[0];
                    int particion = Integer.parseInt(datos[1]);
                    String key = tabla + "_" + particion;
                    replicas.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>()).add(nodoId);
                    loadBalancer.addNodo(key, nodoId);
                }
            });

            NodoHandler nuevoNodo = new NodoHandler(nodoId, ipNodo, puertoNodo);
            nodos.put(nodoId, nuevoNodo);
            loadBalancer.actualizarMetricas(nodoId, nuevoNodo);
            ultimoHeartbeat.put(nodoId, System.currentTimeMillis());
            out.println("REGISTRO_EXITOSO");
            System.out.printf("[Servidor] Nodo %d registrado (IP: %s, Puerto: %d)%n", nodoId, ipNodo, puertoNodo);

        } else if ("HEARTBEAT".equals(partes[0]) && partes.length >= 2) {
            int nodoId = Integer.parseInt(partes[1]);
            ultimoHeartbeat.put(nodoId, System.currentTimeMillis());
            NodoHandler nodo = nodos.get(nodoId);
            if (nodo != null) {
                nodo.setActivo(true);
            }
        }
    }

    private void iniciarHeartbeat() {
        heartbeatScheduler.scheduleAtFixedRate(() -> {
            System.out.println("[Servidor] Verificando heartbeats...");
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

            socket.setSoTimeout(30000);

            String mensaje;
            while ((mensaje = in.readLine()) != null && !mensaje.isEmpty()) {
                System.out.println("[Servidor] Solicitud cliente: " + mensaje);
                String[] partes = mensaje.split("\\|", 4);

                try {
                    switch (partes[0]) {
                        case "CONSULTAR_SALDO" -> {
                            if (partes.length >= 2) {
                                consultarSaldoConFailover(partes[1], out);
                            } else {
                                out.println("ERROR|FORMATO_INVALIDO");
                            }
                        }
                        case "TRANSFERIR_FONDOS" -> {
                            if (partes.length >= 4) {
                                transferirFondosConConsistencia(partes[1], partes[2], partes[3], out);
                            } else {
                                out.println("ERROR|FORMATO_INVALIDO");
                            }
                        }
                        case "ARQUEO" -> arqueoGlobal(out);
                        default -> out.println("ERROR|OPERACION_NO_SOPORTADA");
                    }
                } catch (Exception e) {
                    System.err.println("[Servidor] Error procesando solicitud: " + e.getMessage());
                    out.println("ERROR|ERROR_INTERNO");
                }

                out.flush();
                break;
            }
        } catch (IOException e) {
            System.err.println("Error con cliente: " + e.getMessage());
        }
    }

    private void consultarSaldoConFailover(String idCuenta, PrintWriter out) {
        int particion = hashParticion(idCuenta);
        List<Integer> nodosReplica = replicas.get("CUENTA_" + particion);

        if (nodosReplica == null || nodosReplica.isEmpty()) {
            System.err.println("[ERROR] No hay réplicas para partición CUENTA_" + particion);
            out.println("ERROR|PARTICION_NO_ENCONTRADA");
            return;
        }

        for (Integer nodoId : nodosReplica) {
            NodoHandler nodo = nodos.get(nodoId);
            if (nodo != null && nodo.estaActivo()) {
                try {
                    String respuesta = nodo.enviarYRecibir("CONSULTAR|" + idCuenta, 5);
                    System.out.println("[Servidor] Respuesta nodo " + nodoId + ": " + respuesta);
                    out.println(respuesta);
                    return;
                } catch (TimeoutException e) {
                    System.err.println("[ERROR] Timeout consultando nodo " + nodoId);
                    nodo.setActivo(false);
                    continue;
                }
            }
        }
        System.err.println("[ERROR] Todos los nodos inactivos para CUENTA_" + particion);
        out.println("ERROR|TODOS_LOS_NODOS_INACTIVOS");
    }

    private void transferirFondosConConsistencia(String origen, String destino, String monto, PrintWriter out) {
        int particionOrigen = hashParticion(origen);
        int particionDestino = hashParticion(destino);

        if (particionOrigen == particionDestino) {
            List<Integer> nodosOrigen = replicas.get("CUENTA_" + particionOrigen);
            if (nodosOrigen == null || nodosOrigen.isEmpty()) {
                out.println("ERROR|PARTICION_NO_ENCONTRADA");
                return;
            }

            for (Integer nodoId : nodosOrigen) {
                NodoHandler nodo = nodos.get(nodoId);
                if (nodo != null && nodo.estaActivo()) {
                    try {
                        String respuesta = nodo.enviarYRecibir("TRANSFERIR|" + origen + "|" + destino + "|" + monto,
                                10);
                        out.println(respuesta);
                        return;
                    } catch (TimeoutException e) {
                        nodo.setActivo(false);
                        continue;
                    }
                }
            }
            out.println("ERROR|NODOS_NO_DISPONIBLES");
        } else {
            out.println("ERROR|TRANSFERENCIA_ENTRE_PARTICIONES_NO_SOPORTADA");
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
            int clienteId = Integer.parseInt(id) - 100;
            return (Math.abs(clienteId) % 3) + 1;
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

                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

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
        try {
            new ServidorCentral(5000).iniciar();
        } catch (Exception e) {
            System.err.println("[Servidor] Error crítico: " + e.getMessage());
            e.printStackTrace();
        }
    }
}