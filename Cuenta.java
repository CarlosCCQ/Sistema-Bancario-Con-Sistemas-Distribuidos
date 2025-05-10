public class Cuenta {
    public int idCuenta;
    public int idCliente;
    public double saldo;
    public String tipo;

    public Cuenta(int idCuenta, int idCliente, double saldo, String tipo){
        this.idCuenta = idCuenta;
        this.idCliente = idCliente;
        this.saldo = saldo;
        this.tipo = tipo;
    }

    public synchronized boolean retirar(double monto){
        if(saldo >= monto){
            saldo -= monto;
            return true;
        }
        return false;
    }

    public synchronized void depositar(double monto){
        saldo += monto;
    }

    public synchronized double getSaldo(){
        return saldo;
    }
}
