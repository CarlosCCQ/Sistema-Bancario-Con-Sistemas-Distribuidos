import json
import socket
import threading
import time
import hashlib
from pathlib import Path
import sys

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
                        if linea.strip():
                            partes = linea.strip().split('|')
                            if len(partes) >= 4:
                                if partes[0] == 'CLIENTE':
                                    self.clientes[int(partes[1])] = {
                                        'id': int(partes[1]),
                                        'nombre': partes[2],
                                        'email': partes[3],
                                        'telefono': partes[4]
                                    }
                                elif partes[0] == 'CUENTA':
                                    self.cuentas[int(partes[1])] = {
                                        'id': int(partes[1]),
                                        'id_cliente': int(partes[2]),
                                        'saldo': float(partes[3]),
                                        'tipo': partes[4]
                                    }
                                elif partes[0] == 'TRANSACCION' and len(partes) >= 7:
                                    self.transacciones[int(partes[1])] = {
                                        'id': int(partes[1]),
                                        'id_origen': int(partes[2]),
                                        'id_destino': int(partes[3]),
                                        'monto': float(partes[4]),
                                        'fecha_hora': partes[5],
                                        'estado': partes[6]
                                    }
        except Exception as e:
            print(f"Error cargando {self.ruta}: {str(e)}")

    def guardar_datos(self):
        try:
            with self.lock:
                temp = self.ruta.with_suffix('.tmp')
                with open(temp, 'w', encoding='utf-8') as f:
                    for cliente_id, datos in self.clientes.items():
                        f.write(f"CLIENTE|{cliente_id}|{datos['nombre']}|{datos['email']}|{datos['telefono']}\n")
                    
                    for cuenta_id, datos in self.cuentas.items():
                        f.write(f"CUENTA|{cuenta_id}|{datos['id_cliente']}|{datos['saldo']:.2f}|{datos['tipo']}\n")
                    
                    for trans_id, datos in self.transacciones.items():
                        f.write(f"TRANSACCION|{trans_id}|{datos['id_origen']}|{datos['id_destino']}|{datos['monto']:.2f}|{datos['fecha_hora']}|{datos['estado']}\n")
                
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
        threading.Thread(target=self.verificar_y_reparar_replicas, daemon=True).start()

    def cargar_particiones(self):
        self.ruta_datos.mkdir(parents=True, exist_ok=True)
        
        if self.id == 1:
            particiones_asignadas = [(1, [1, 2]), (2, [3]), (3, [1])]
        else:
            particiones_asignadas = [(1, [3]), (2, [1, 2]), (3, [2, 3])]
        
        for particion_id, replicas_list in particiones_asignadas:
            for replica in replicas_list:
                archivo = self.ruta_datos / f"particion_{particion_id}_rep{replica}.dat"
                if particion_id not in self.particiones:
                    self.particiones[particion_id] = Particion(particion_id, archivo)
                    if not archivo.exists():
                        self.particiones[particion_id].guardar_datos()

    def hash_particion(self, id_cuenta):
        return (abs(int(id_cuenta)) % TAMANO_PARTICIONES) + 1

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
            elif partes[0] == 'SINCRONIZAR':
                return self.actualizar_particion(int(partes[1]), partes[2])
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
            
            trans_id = len(self.particiones[particion_id].transacciones) + 1
            self.particiones[particion_id].transacciones[trans_id] = {
                'id': trans_id,
                'id_origen': origen,
                'id_destino': destino,
                'monto': monto,
                'fecha_hora': time.strftime(FORMATO_FECHA),
                'estado': 'CONFIRMADA'
            }
            
            self.particiones[particion_id].guardar_datos()
            self.sincronizar_replica(particion_id)
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
    
    def sincronizar_replica(self, particion_id):
        try:
            otro_nodo = 2 if self.id == 1 else 1
            ip_otro = "localhost"
            puerto_otro = 6000 if otro_nodo == 1 else 6001
            
            datos = json.dumps(self.particiones[particion_id].cuentas)
            mensaje = f"SINCRONIZAR|{particion_id}|{datos}"
            
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3)
            sock.connect((ip_otro, puerto_otro))
            sock.send(mensaje.encode('utf-8'))
            sock.close()
        except Exception as e:
            print(f"Error sincronizando con nodo {otro_nodo}: {str(e)}")
    
    def verificar_y_reparar_replicas(self):
        while self.activo:
            try:
                time.sleep(30)
                otro_nodo = 2 if self.id == 1 else 1
                ip_otro = "localhost"
                puerto_otro = 6000 if otro_nodo == 1 else 6001
                
                for particion_id in self.particiones.keys():
                    try:
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.settimeout(5)
                        sock.connect((ip_otro, puerto_otro))
                        sock.send(f"OBTENER_PARTICION|{particion_id}".encode('utf-8'))
                        respuesta = sock.recv(4096).decode('utf-8')
                        sock.close()
                        
                        if respuesta and not respuesta.startswith("ERROR"):
                            with self.particiones[particion_id].lock:
                                datos_remotos = json.loads(respuesta)
                                datos_locales = self.particiones[particion_id].cuentas
                                
                                if datos_remotos != datos_locales:
                                    for cuenta_id, datos_cuenta in datos_remotos.items():
                                        if cuenta_id not in datos_locales:
                                            datos_locales[cuenta_id] = datos_cuenta
                                    self.particiones[particion_id].guardar_datos()
                    except Exception as e:
                        continue
            except Exception as e:
                continue

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