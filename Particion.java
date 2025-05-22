import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Particion {
    private final Path archivo;
    private final Map<Integer, Cliente> clientes = new HashMap<>();
    private final Map<Integer, Cuenta> cuentas = new HashMap<>();
    private final Map<Integer, Transaccion> transacciones = new HashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    public Particion(String rutaArchivo) {
        this.archivo = Paths.get(rutaArchivo);
        try {
            Files.createDirectories(archivo.getParent());
            if (!Files.exists(archivo)) {
                Files.createFile(archivo);
            }
        } catch (IOException e) {
            System.err.println("Error creando archivo: " + e.getMessage());
        }
        cargarDatos();
    }

    private void cargarDatos() {
        lock.writeLock().lock();
        try (BufferedReader br = Files.newBufferedReader(archivo)) {
            br.lines().forEach(linea -> {
                String[] partes = linea.split("\\|");
                switch (partes[0]) {
                    case "CLIENTE":
                        clientes.put(Integer.parseInt(partes[1]), new Cliente(
                                Integer.parseInt(partes[1]),
                                partes[2],
                                partes[3],
                                partes[4]));
                        break;

                    case "CUENTA":
                        cuentas.put(Integer.parseInt(partes[1]), new Cuenta(
                                Integer.parseInt(partes[1]),
                                Integer.parseInt(partes[2]),
                                Double.parseDouble(partes[3]),
                                partes[4]));
                        break;

                    case "TRANSACCION":
                        transacciones.put(Integer.parseInt(partes[1]), new Transaccion(
                                Integer.parseInt(partes[1]),
                                Integer.parseInt(partes[2]),
                                Integer.parseInt(partes[3]),
                                Double.parseDouble(partes[4]),
                                LocalDateTime.parse(partes[5], DATE_FORMATTER),
                                partes[6]));
                        break;
                }
            });
        } catch (IOException e) {
            System.err.println("Error cargando datos: " + e.getMessage());
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void fusionarDatos(String datosRemotos) {
        try {
            lock.writeLock().lock();

            String[] partes = datosRemotos.split("\\{\"cuentas\":\\{")[1].split("\\}\\}")[0].split(",");
            for (String parte : partes) {
                if (parte.trim().isEmpty())
                    continue;

                String[] keyValue = parte.split("\":");
                int id = Integer.parseInt(keyValue[0].replace("\"", ""));

                if (!cuentas.containsKey(id)) {
                    String value = keyValue[1];
                    String[] campos = value.split(",");
                    int idCliente = Integer.parseInt(campos[0].split(":")[1]);
                    double saldo = Double.parseDouble(campos[1].split(":")[1]);
                    String tipo = campos[2].split(":")[1].replace("\"", "").replace("}", "");

                    cuentas.put(id, new Cuenta(id, idCliente, saldo, tipo));
                }
            }
            guardarCambios();
        } catch (Exception e) {
            System.err.println("Error fusionando datos: " + e.getMessage());
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean transferir(int idOrigen, int idDestino, double monto) {
        lock.writeLock().lock();
        try {
            Cuenta origen = cuentas.get(idOrigen);
            Cuenta destino = cuentas.get(idDestino);
            boolean exito = false;

            if (origen != null && destino != null && origen.getSaldo() >= monto) {
                origen.retirar(monto);
                destino.depositar(monto);
                exito = true;
            }
            Transaccion t = new Transaccion(
                    transacciones.size() + 1,
                    idOrigen,
                    idDestino,
                    monto,
                    exito ? "CONFIRMADA" : "RECHAZADA");
            transacciones.put(t.getId(), t);

            guardarCambios();
            return exito;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void guardarCambios() {
        try (BufferedWriter bw = Files.newBufferedWriter(archivo, StandardOpenOption.TRUNCATE_EXISTING)) {
            for (Cliente cliente : clientes.values()) {
                bw.write(String.format("CLIENTE|%d|%s|%s|%s%n",
                        cliente.getId(), cliente.getNombre(), cliente.getEmail(), cliente.getTelefono()));
            }
            for (Cuenta cuenta : cuentas.values()) {
                bw.write(String.format("CUENTA|%d|%d|%.2f|%s%n",
                        cuenta.getId(), cuenta.getIdCliente(), cuenta.getSaldo(), cuenta.getTipoCuenta()));
            }
            for (Transaccion t : transacciones.values()) {
                bw.write(String.format("TRANSACCION|%d|%d|%d|%.2f|%s|%s%n",
                        t.getId(),
                        t.getIdOrigen(),
                        t.getIdDestino(),
                        t.getMonto(),
                        t.getFechaHora().format(DATE_FORMATTER),
                        t.getEstado()));
            }
        } catch (IOException e) {
            System.err.println("Error guardando cambios: " + e.getMessage());
        }
    }

    public double arqueoLocal() {
        lock.readLock().lock();
        try {
            return cuentas.values().stream().mapToDouble(Cuenta::getSaldo).sum();
        } finally {
            lock.readLock().unlock();
        }
    }

    public Cuenta getCuenta(int idCuenta) {
        lock.readLock().lock();
        try {
            return cuentas.get(idCuenta);
        } finally {
            lock.readLock().unlock();
        }
    }

    public String obtenerDatosJson() {
        lock.readLock().lock();
        try {
            StringBuilder json = new StringBuilder("{\"cuentas\":{");
            boolean first = true;
            for (Map.Entry<Integer, Cuenta> entry : cuentas.entrySet()) {
                if (!first)
                    json.append(",");
                Cuenta c = entry.getValue();
                json.append(String.format("\"%d\":{\"id_cliente\":%d,\"saldo\":%.2f,\"tipo\":\"%s\"}",
                        c.getId(), c.getIdCliente(), c.getSaldo(), c.getTipoCuenta()));
                first = false;
            }
            json.append("}}");
            return json.toString();
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean actualizarDesdeJson(String datos) {
        try {
            lock.writeLock().lock();
            cuentas.clear();

            String[] partes = datos.split("\\{\"cuentas\":\\{")[1].split("\\}\\}")[0].split(",");
            for (String parte : partes) {
                if (parte.trim().isEmpty())
                    continue;

                String[] keyValue = parte.split("\":");
                int id = Integer.parseInt(keyValue[0].replace("\"", ""));
                String value = keyValue[1];

                String[] campos = value.split(",");
                int idCliente = Integer.parseInt(campos[0].split(":")[1]);
                double saldo = Double.parseDouble(campos[1].split(":")[1]);
                String tipo = campos[2].split(":")[1].replace("\"", "").replace("}", "");

                cuentas.put(id, new Cuenta(id, idCliente, saldo, tipo));
            }
            guardarCambios();
            return true;
        } catch (Exception e) {
            System.err.println("Error actualizando desde JSON: " + e.getMessage());
            return false;
        } finally {
            lock.writeLock().unlock();
        }
    }
}