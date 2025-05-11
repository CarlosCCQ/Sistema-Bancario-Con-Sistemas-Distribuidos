package src;
public class Cliente {
    private final int id;
    private final String nombre;
    private final String email;
    private final String telefono;

    public Cliente(int id, String nombre, String email, String telefono){
        this.id = id;
        this.nombre = nombre;
        this.email = email;
        this.telefono = telefono;
    }

    public int getId(){return id;}
    public String getNombre(){return nombre;}
    public String getEmail(){return email;}
    public String getTelefono(){return telefono;}
}
