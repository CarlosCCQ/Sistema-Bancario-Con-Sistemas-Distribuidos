public class Transaccion {
    public int id;
    public int idOrigen;
    public int idDestino;
    public double monto;
    public String fechaHora;
    public String estado;

    public Transaccion(int id, int idOrigen, int idDestino, double monto, String fechaHora, String estado){
        this.id = id;
        this.idOrigen = idOrigen;
        this.idDestino = idDestino;
        this.monto = monto;
        this.fechaHora = fechaHora;
        this.estado = estado;
    }

    @Override
    public String toString(){
        return id + "," + idOrigen + "," + idDestino + "," + monto + "," + fechaHora + "," + estado;
    }

    public static Transaccion fromString(String line){
        String[] parts = line.split(",");
        return new Transaccion(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), Integer.parseInt(parts[2]), Double.parseDouble(parts[3]), parts[4], parts[5]);
    }
}
