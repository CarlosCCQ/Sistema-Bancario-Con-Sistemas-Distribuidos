import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ClienteBanco {
    private static final AtomicInteger transaccionesExitosas = new AtomicInteger();
    private static final String[] OPERACIONES = { "CONSULTAR_SALDO", "TRANSFERIR_FONDOS" };
    private static final Random rand = new Random();

    public static void main(String[] args) {
        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();

        for (int i = 0; i < 1000; i++) {
            final int clienteId = i;
            pool.execute(() -> {
                try {
                    Thread.sleep(rand.nextInt(2000) + 1000);

                    try (Socket socket = new Socket("localhost", 5000)) {
                        socket.setSoTimeout(10000);

                        try (PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                                BufferedReader in = new BufferedReader(
                                        new InputStreamReader(socket.getInputStream()))) {

                            String operacion = OPERACIONES[rand.nextInt(OPERACIONES.length)];
                            String mensaje = generarMensaje(operacion);

                            out.println(mensaje);
                            out.flush();

                            String respuesta = in.readLine();

                            if (respuesta != null && !respuesta.startsWith("ERROR")) {
                                transaccionesExitosas.incrementAndGet();
                            }

                            Thread.sleep(rand.nextInt(1000));
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Cliente " + clienteId + " falló: " + e.getMessage());
                }
            });
        }

        pool.shutdown();
        try {
            pool.awaitTermination(2, TimeUnit.MINUTES);
            System.out.println("\nResultados:");
            System.out.println("Transacciones exitosas: " + transaccionesExitosas.get());
            System.out.println("Transacciones fallidas: " + (1000 - transaccionesExitosas.get()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String generarMensaje(String operacion) {
        int cuentaId = 101 + rand.nextInt(1000);
        return switch (operacion) {
            case "CONSULTAR_SALDO" -> "CONSULTAR_SALDO|" + cuentaId;
            case "TRANSFERIR_FONDOS" ->
                String.format("TRANSFERIR_FONDOS|%d|%d|%.2f",
                        cuentaId,
                        101 + rand.nextInt(1000),
                        rand.nextDouble() * 1000);
            default -> throw new IllegalArgumentException();
        };
    }
}