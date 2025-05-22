import java.io.*;
import java.nio.file.*;
import java.util.Random;

public class InicializadorDatos {
    private static final Random random = new Random();
    private static final String[] NOMBRES = { "Juan", "María", "Carlos", "Ana", "Luis", "Carmen", "Pedro", "Laura" };
    private static final String[] APELLIDOS = { "Pérez", "López", "García", "Martín", "González", "Rodríguez",
            "Sánchez", "Díaz" };
    private static final String[] TIPOS_CUENTA = { "Ahorros", "Corriente" };

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Uso: java InicializadorDatos <ruta_datos>");
            System.exit(1);
        }

        String rutaDatos = args[0];
        try {
            Files.createDirectories(Paths.get(rutaDatos));
            crearDatosIniciales(rutaDatos);
            System.out.println("Datos iniciales creados correctamente");
        } catch (IOException e) {
            System.err.println("Error creando datos: " + e.getMessage());
        }
    }

    private static void crearDatosIniciales(String rutaDatos) throws IOException {
        crearParticion1(rutaDatos);
        crearParticion2(rutaDatos);
        crearParticion3(rutaDatos);
    }

    private static void crearParticion1(String rutaDatos) throws IOException {
        String[] archivos = {
                rutaDatos + "/particion_1_rep1.dat",
                rutaDatos + "/particion_1_rep2.dat",
                rutaDatos + "/particion_1_rep3.dat"
        };

        for (String archivo : archivos) {
            try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(archivo))) {
                for (int i = 1; i <= 1000; i++) {
                    int clienteId = i;
                    int cuentaId = 100 + i;

                    if (hashParticion(clienteId) == 1) {
                        String nombre = NOMBRES[random.nextInt(NOMBRES.length)] + " "
                                + APELLIDOS[random.nextInt(APELLIDOS.length)];
                        String email = nombre.toLowerCase().replace(" ", "") + "@email.com";
                        String telefono = "9" + String.format("%08d", random.nextInt(100000000));

                        writer.write(String.format("CLIENTE|%d|%s|%s|%s%n", clienteId, nombre, email, telefono));

                        double saldo = 1000 + (random.nextDouble() * 9000);
                        String tipo = TIPOS_CUENTA[random.nextInt(TIPOS_CUENTA.length)];

                        writer.write(String.format("CUENTA|%d|%d|%.2f|%s%n", cuentaId, clienteId, saldo, tipo));
                    }
                }
            }
        }
    }

    private static void crearParticion2(String rutaDatos) throws IOException {
        String[] archivos = {
                rutaDatos + "/particion_2_rep1.dat",
                rutaDatos + "/particion_2_rep2.dat",
                rutaDatos + "/particion_2_rep3.dat"
        };

        for (String archivo : archivos) {
            try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(archivo))) {
                for (int i = 1; i <= 1000; i++) {
                    int clienteId = i;
                    int cuentaId = 100 + i;

                    if (hashParticion(clienteId) == 2) {
                        String nombre = NOMBRES[random.nextInt(NOMBRES.length)] + " "
                                + APELLIDOS[random.nextInt(APELLIDOS.length)];
                        String email = nombre.toLowerCase().replace(" ", "") + "@email.com";
                        String telefono = "9" + String.format("%08d", random.nextInt(100000000));

                        writer.write(String.format("CLIENTE|%d|%s|%s|%s%n", clienteId, nombre, email, telefono));

                        double saldo = 1000 + (random.nextDouble() * 9000);
                        String tipo = TIPOS_CUENTA[random.nextInt(TIPOS_CUENTA.length)];

                        writer.write(String.format("CUENTA|%d|%d|%.2f|%s%n", cuentaId, clienteId, saldo, tipo));
                    }
                }
            }
        }
    }

    private static void crearParticion3(String rutaDatos) throws IOException {
        String[] archivos = {
                rutaDatos + "/particion_3_rep1.dat",
                rutaDatos + "/particion_3_rep2.dat",
                rutaDatos + "/particion_3_rep3.dat"
        };

        for (String archivo : archivos) {
            try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(archivo))) {
                for (int i = 1; i <= 1000; i++) {
                    int clienteId = i;
                    int cuentaId = 100 + i;

                    if (hashParticion(clienteId) == 3) {
                        String nombre = NOMBRES[random.nextInt(NOMBRES.length)] + " "
                                + APELLIDOS[random.nextInt(APELLIDOS.length)];
                        String email = nombre.toLowerCase().replace(" ", "") + "@email.com";
                        String telefono = "9" + String.format("%08d", random.nextInt(100000000));

                        writer.write(String.format("CLIENTE|%d|%s|%s|%s%n", clienteId, nombre, email, telefono));

                        double saldo = 1000 + (random.nextDouble() * 9000);
                        String tipo = TIPOS_CUENTA[random.nextInt(TIPOS_CUENTA.length)];

                        writer.write(String.format("CUENTA|%d|%d|%.2f|%s%n", cuentaId, clienteId, saldo, tipo));
                    }
                }
            }
        }
    }

    private static int hashParticion(int id) {
        return (Math.abs(id) % 3) + 1;
    }
}