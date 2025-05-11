import socket
import threading
import json
import hashlib
from pathlib import Path
from datetime import datetime
import time

FORMATO_FECHA = "%Y-%m-%d %H:%M:%S"
TAMANO_PARTICIONES = 3
REPLICAS = 3
HEARTBEAT_INTERVAL = 5

class Particion:
    def __init__(self, id_particion, ruta_archivo):
        self.id = id_particion
        self.ruta = Path(ruta_archivo)
        self.cuentas = {}
        self.clientes = {}
        self.transacciones = {}
        self.lock = threading.Lock()
        self.cargar_datos()

    def cargar_datos(self):
        try:
            with self.lock:
                if not self.ruta.exists():
                    self.ruta.touch()
                
                with open(self.ruta, 'r', encoding='utf-8') as f:
                    for linea in f:
                        data = json.loads(linea.strip())
                        if data['tipo'] == 'cuenta':
                            self.cuentas[data['id']] = {
                                'id_cliente': data['id_cliente'],
                                'saldo': data['saldo'],
                                'tipo': data['tipo']
                            }
        except Exception as e:
            print(f"Error cargando {self.ruta}: {str(e)}")

    def guardar_datos(self):
        try:
            with self.lock:
                temp = self.ruta.with_suffix('.tmp')
                with open(temp, 'w', encoding='utf-8') as f:
                    for cuenta_id, datos in self.cuentas.items():
                        json.dump({
                            'tipo': 'cuenta',
                            'id': cuenta_id,
                            'id_cliente': datos['id_cliente'],
                            'saldo': datos['saldo'],
                            'tipo': datos['tipo']
                        }, f)
                        f.write('\n')
                temp.replace(self.ruta)
        except Exception as e:
            print(f"Error guardando {self.ruta}: {str(e)}")

class NodoTrabajador:
    def __init__(self, id_nodo, ip_servidor, puerto_servidor, ip_nodo, puerto_nodo, ruta_datos):
        self.id = id_nodo
        self.ip_servidor = ip_servidor
        self.puerto_servidor = puerto_servidor
        self.ip = ip_nodo
        self.puerto = puerto_nodo
        self.ruta_datos = Path(ruta_datos)
        self.particiones = {}
        self.activo = True
        self.cargar_particiones()
        
        self.sock_servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.conectar_servidor()
        
        threading.Thread(target=self.iniciar_servidor, daemon=True).start()
        threading.Thread(target=self.enviar_heartbeat, daemon=True).start()

    def cargar_particiones(self):
        self.ruta_datos.mkdir(parents=True, exist_ok=True)
        for p in range(1, TAMANO_PARTICIONES + 1):
            for r in range(1, REPLICAS + 1):
                archivo = self.ruta_datos / f"particion_{p}_rep{r}.dat"
                if archivo.exists():
                    self.particiones[p] = Particion(p, archivo)
                else:
                    self.particiones[p] = Particion(p, archivo)
                    self.particiones[p].guardar_datos()

    def hash_particion(self, id_cuenta):
        return (int(hashlib.md5(str(id_cuenta).encode()).hexdigest(), 16) % TAMANO_PARTICIONES) + 1

    def conectar_servidor(self):
        try:
            self.sock_servidor.connect((self.ip_servidor, self.puerto_servidor + 1))
            self.registrar_en_servidor()
        except Exception as e:
            print(f"Error conectando al servidor: {str(e)}")
            self.reconectar()

    def registrar_en_servidor(self):
        particiones_str = ",".join([f"CUENTA:{p}" for p in self.particiones.keys()])
        mensaje = f"REGISTRO|{self.id}|{self.ip}|{self.puerto}|{particiones_str}"
        self.sock_servidor.send(mensaje.encode('utf-8'))

    def iniciar_servidor(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.ip, self.puerto))
            s.listen()
            
            while self.activo:
                conn, addr = s.accept()
                threading.Thread(target=self.manejar_conexion, args=(conn,), daemon=True).start()

    def manejar_conexion(self, conn):
        with conn:
            try:
                data = conn.recv(1024).decode('utf-8').strip()
                if not data:
                    return
                
                respuesta = self.procesar_comando(data)
                conn.send(respuesta.encode('utf-8'))
            except Exception as e:
                print(f"Error en conexión: {str(e)}")

    def procesar_comando(self, comando):
        try:
            partes = comando.split('|')
            if partes[0] == 'CONSULTAR':
                return self.consultar_saldo(int(partes[1]))
            elif partes[0] == 'TRANSFERIR':
                return self.procesar_transferencia(int(partes[1]), int(partes[2]), float(partes[3]))
            elif partes[0] == 'ARQUEO':
                return f"ARQUEO|{self.calcular_arqueo():.2f}"
            elif partes[0] == 'BLOQUEAR_ARQUEO':
                return "OK"
            elif partes[0] == 'DESBLOQUEAR_ARQUEO':
                return "OK"
            elif partes[0] == 'OBTENER_PARTICION':
                return self.obtener_particion(int(partes[1]))
            elif partes[0] == 'ACTUALIZAR_PARTICION':
                return self.actualizar_particion(int(partes[1]), '|'.join(partes[2:]))
            else:
                return "ERROR|COMANDO_INVALIDO"
        except Exception as e:
            return f"ERROR|{str(e)}"

    def consultar_saldo(self, id_cuenta):
        particion_id = self.hash_particion(id_cuenta)
        if particion_id not in self.particiones:
            return "ERROR|PARTICION_NO_LOCAL"
        
        with self.particiones[particion_id].lock:
            cuenta = self.particiones[particion_id].cuentas.get(id_cuenta)
            return f"SALDO|{cuenta['saldo']}" if cuenta else "ERROR|CUENTA_NO_EXISTE"

    def procesar_transferencia(self, origen, destino, monto):
        particion_id = self.hash_particion(origen)
        if particion_id not in self.particiones:
            return "ERROR|PARTICION_NO_LOCAL"
        
        with self.particiones[particion_id].lock:
            cuenta_origen = self.particiones[particion_id].cuentas.get(origen)
            cuenta_destino = self.particiones[particion_id].cuentas.get(destino)
            
            if not cuenta_origen or not cuenta_destino:
                return "ERROR|CUENTA_NO_EXISTE"
            
            if cuenta_origen['saldo'] < monto:
                return "ERROR|SALDO_INSUFICIENTE"
            
            cuenta_origen['saldo'] -= monto
            cuenta_destino['saldo'] += monto
            self.particiones[particion_id].guardar_datos()
            return "OK"

    def calcular_arqueo(self):
        total = 0.0
        for particion in self.particiones.values():
            with particion.lock:
                total += sum(c['saldo'] for c in particion.cuentas.values())
        return total

    def obtener_particion(self, particion_id):
        if particion_id not in self.particiones:
            return "ERROR|PARTICION_NO_EXISTE"
        
        with self.particiones[particion_id].lock:
            return json.dumps(self.particiones[particion_id].cuentas)

    def actualizar_particion(self, particion_id, datos):
        if particion_id not in self.particiones:
            return "ERROR|PARTICION_NO_EXISTE"
        
        try:
            nueva_data = json.loads(datos)
            with self.particiones[particion_id].lock:
                self.particiones[particion_id].cuentas = nueva_data
                self.particiones[particion_id].guardar_datos()
            return "OK"
        except Exception as e:
            return f"ERROR|{str(e)}"

    def enviar_heartbeat(self):
        while self.activo:
            try:
                self.sock_servidor.send(f"HEARTBEAT|{self.id}".encode('utf-8'))
                time.sleep(HEARTBEAT_INTERVAL)
            except Exception as e:
                print(f"Error enviando heartbeat: {str(e)}")
                self.reconectar()
                time.sleep(HEARTBEAT_INTERVAL)

    def reconectar(self):
        while self.activo:
            try:
                self.sock_servidor.close()
                self.sock_servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock_servidor.connect((self.ip_servidor, self.puerto_servidor + 1))
                self.registrar_en_servidor()
                return
            except Exception as e:
                time.sleep(5)

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 7:
        print("Uso: python nodo.py <id> <ip_servidor> <puerto_servidor> <ip_nodo> <puerto_nodo> <ruta_datos>")
        sys.exit(1)
    
    nodo = NodoTrabajador(
        id_nodo=int(sys.argv[1]),
        ip_servidor=sys.argv[2],
        puerto_servidor=int(sys.argv[3]),
        ip_nodo=sys.argv[4],
        puerto_nodo=int(sys.argv[5]),
        ruta_datos=sys.argv[6]
    )
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nDeteniendo nodo...")
        nodo.activo = False
        nodo.sock_servidor.close()