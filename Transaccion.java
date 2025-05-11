
import java.time.LocalDateTime;

public class Transaccion {
    private final int id;
    private final int idOrigen;
    private final int idDestino;
    private final double monto;
    private final LocalDateTime fechaHora;
    private String estado;

    public Transaccion(int id, int idOrigen, int idDestino, double monto, String estado) {
        this.id = id;
        this.idOrigen = idOrigen;
        this.idDestino = idDestino;
        this.monto = monto;
        this.fechaHora = LocalDateTime.now();
        this.estado = estado;
    }

    public Transaccion(int id, int idOrigen, int idDestino, double monto, LocalDateTime fechaHora, String estado) {
        this.id = id;
        this.idOrigen = idOrigen;
        this.idDestino = idDestino;
        this.monto = monto;
        this.fechaHora = fechaHora;
        this.estado = estado;
    }

    public int getId() { return id; }
    public int getIdOrigen() { return idOrigen; }
    public int getIdDestino() { return idDestino; }
    public double getMonto() { return monto; }
    public LocalDateTime getFechaHora() { return fechaHora; }
    public String getEstado() { return estado; }
    public void setEstado(String estado) { this.estado = estado; }
}
