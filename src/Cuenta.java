package src;
public class Cuenta {
    private final int id;
    private final int idCliente;
    private double saldo;
    private final String tipoCuenta;

    public Cuenta(int id, int idCliente, double saldo, String tipoCuenta){
        this.id = id;
        this.idCliente = idCliente;
        this.saldo = saldo;
        this.tipoCuenta = tipoCuenta;
    }

    public int getId(){return id;}
    public int getIdCliente(){return idCliente;}
    public double getSaldo(){return saldo;}
    public String getTipoCuenta(){return tipoCuenta;}

    public synchronized void retirar(double monto){saldo -=monto;}
    public synchronized void depositar(double monto){saldo +=monto;}
}
