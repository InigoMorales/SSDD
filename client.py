"""
client.py - Cliente del servicio de mensajeria distribuido
Sistemas Distribuidos - UC3M - Curso 2025-2026

Implementa el protocolo de comunicacion con el servidor via sockets TCP.
Soporta las operaciones: REGISTER, UNREGISTER, CONNECT, DISCONNECT, USERS, SEND, SENDATTACH.

Protocolo: todas las cadenas se envian terminadas en '\0' (null-terminated).
Los codigos de respuesta del servidor se reciben como un byte.
"""

import socket
import threading
import argparse
import sys
import os
from enum import Enum

from web_service import call_normalize


class client:

    # ==================== TIPOS ====================

    class RC(Enum):
        OK = 0
        ERROR = 1
        USER_ERROR = 2

    # ==================== ATRIBUTOS ====================

    _server = None          # IP del servidor
    _port = -1              # Puerto del servidor
    _listen_port = None     # Puerto local de escucha para recibir mensajes
    _listen_thread = None   # Hilo que escucha mensajes entrantes del servidor
    _listen_socket = None   # Socket de escucha del cliente (thread receptor)
    _connected_user = None  # Nombre del usuario actualmente conectado
    _stop_event = threading.Event()  # Evento para detener el hilo de escucha

    # ==================== METODOS DE UTILIDAD ====================

    @staticmethod
    def _send_string(sock, text):
        """
        Envia una cadena por el socket terminada en el caracter nulo '\0'.
        El servidor espera todas las cadenas con este formato.
        """
        msg = (text + '\0').encode('utf-8')
        sock.sendall(msg)

    @staticmethod
    def _recv_string(sock, max_length=4096):
        """
        Recibe una cadena del socket hasta encontrar el terminador nulo '\0'.
        Lee byte a byte para no consumir datos extra del buffer.
        """
        result = bytearray()
        while len(result) < max_length:
            byte = sock.recv(1)
            if not byte or byte == b'\0':
                break
            result += byte
        if len(result) >= max_length:
            raise ConnectionError("Maximum string length exceeded (missing null terminator)")
        return result.decode('utf-8')

    @staticmethod
    def _recv_byte(sock):
        """
        Recibe un byte del socket y lo devuelve como entero.
        Se usa para los codigos de respuesta del servidor.
        """
        data = sock.recv(1)
        if not data:
            return -1
        return data[0]

    @staticmethod
    def _find_free_port():
        """
        Busca un puerto libre en el sistema asignando uno automaticamente
        y devolviendo el numero asignado por el SO.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))
            return s.getsockname()[1]

    @staticmethod
    def _connect_to_server():
        """
        Crea y devuelve un socket TCP conectado al servidor principal.
        Lanza excepcion si no puede conectar.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((client._server, client._port))
        return sock

    # ==================== HILO RECEPTOR DE MENSAJES ====================

    @staticmethod
    def _listener_thread(listen_socket):
        """
        Hilo que se queda escuchando conexiones entrantes del servidor.
        El servidor se conecta a este hilo para entregar mensajes a este cliente.
        Maneja los tipos de mensajes:
          - SEND_MESSAGE     -> mensaje normal de otro usuario
          - SEND_MESS_ACK    -> confirmacion de entrega de un mensaje enviado
          - SEND_MESSAGE_ATTACH -> mensaje con fichero adjunto (Parte 2)
          - SEND_MESS_ATTACH_ACK -> confirmacion de entrega con adjunto (Parte 2)
          - GET_FILE         -> solicitud de transferencia de fichero (Parte 2)
        """
        listen_socket.settimeout(1.0)  # timeout para poder comprobar _stop_event

        while not client._stop_event.is_set():
            try:
                conn, addr = listen_socket.accept()
            except socket.timeout:
                continue
            except Exception:
                break

            try:
                # Recibir el tipo de operacion
                operation = client._recv_string(conn)

                if operation == "SEND_MESSAGE":
                    # El servidor nos envia un mensaje de otro usuario
                    sender   = client._recv_string(conn)
                    msg_id   = client._recv_string(conn)
                    message  = client._recv_string(conn)
                    conn.close()

                    # Mostrar el mensaje recibido segun el formato del enunciado
                    print(f"\ns> MESSAGE {msg_id} FROM {sender}")
                    print(f"   {message}")
                    print("   END")
                    print("c> ", end='', flush=True)

                elif operation == "SEND_MESS_ACK":
                    # El servidor nos confirma que nuestro mensaje llego al destinatario
                    msg_id = client._recv_string(conn)
                    conn.close()

                    print(f"\nc> SEND MESSAGE {msg_id} OK")
                    print("c> ", end='', flush=True)

                elif operation == "SEND_MESSAGE_ATTACH":
                    # Parte 2: mensaje con fichero adjunto
                    sender    = client._recv_string(conn)
                    msg_id    = client._recv_string(conn)
                    message   = client._recv_string(conn)
                    file_name = client._recv_string(conn)
                    conn.close()

                    print(f"\nc> MESSAGE {msg_id} FROM {sender}")
                    print(f"   {message}")
                    print("   END")
                    print(f"   FILE {file_name}")
                    print("c> ", end='', flush=True)

                elif operation == "SEND_MESS_ATTACH_ACK":
                    # Parte 2: confirmacion de entrega con fichero adjunto
                    msg_id    = client._recv_string(conn)
                    file_name = client._recv_string(conn)
                    conn.close()

                    print(f"\nc> SENDATTACH MESSAGE {msg_id} {file_name} OK")
                    print("c> ", end='', flush=True)

                elif operation == "GET_FILE":
                    # Parte 2: otro cliente nos pide un fichero
                    requester = client._recv_string(conn)
                    file_name = client._recv_string(conn)

                    # Saneamiento estricto contra Path Traversal
                    file_name = os.path.basename(file_name)

                    try:
                        with open(file_name, 'rb') as f:
                            data = f.read()
                        # Enviar tamano y contenido
                        size_str = str(len(data))
                        client._send_string(conn, size_str)
                        conn.sendall(data)
                    except Exception as e:
                        # Si no existe el fichero enviamos tamano 0
                        client._send_string(conn, "0")
                    conn.close()

                else:
                    conn.close()

            except Exception as e:
                try:
                    conn.close()
                except Exception:
                    pass

        listen_socket.close()

    # ==================== OPERACIONES DEL CLIENTE ====================

    @staticmethod
    def register(user):
        """
        Registra un usuario nuevo en el servidor.
        Protocolo:
          1. Conectar al servidor
          2. Enviar "REGISTER\0"
          3. Enviar nombre de usuario\0
          4. Recibir 1 byte: 0=OK, 1=ya existe, 2=error
          5. Cerrar conexion
        """
        sock = None
        try:
            sock = client._connect_to_server()
            client._send_string(sock, "REGISTER")
            client._send_string(sock, user)
            result = client._recv_byte(sock)

            if result == 0:
                print("c> REGISTER OK")
                return client.RC.OK
            elif result == 1:
                print("c> USERNAME ALREADY IN USE")
                return client.RC.USER_ERROR
            else:
                print("c> REGISTER FAIL")
                return client.RC.ERROR

        except Exception as e:
            print("c> REGISTER FAIL")
            return client.RC.ERROR
        finally:
            # Cerrar siempre el socket, incluso si hay una excepcion de red
            if sock is not None:
                sock.close()

    @staticmethod
    def unregister(user):
        """
        Da de baja a un usuario del servidor.
        Protocolo:
          1. Conectar al servidor
          2. Enviar "UNREGISTER\0"
          3. Enviar nombre de usuario\0
          4. Recibir 1 byte: 0=OK, 1=no existe, 2=error
          5. Cerrar conexion
        """
        sock = None
        try:
            sock = client._connect_to_server()
            client._send_string(sock, "UNREGISTER")
            client._send_string(sock, user)
            result = client._recv_byte(sock)

            if result == 0:
                print("c> UNREGISTER OK")
                return client.RC.OK
            elif result == 1:
                print("c> USER DOES NOT EXIST")
                return client.RC.USER_ERROR
            else:
                print("c> UNREGISTER FAIL")
                return client.RC.ERROR

        except Exception as e:
            print("c> UNREGISTER FAIL")
            return client.RC.ERROR
        finally:
            # Cerrar siempre el socket, incluso si hay una excepcion de red
            if sock is not None:
                sock.close()

    @staticmethod
    def connect(user):
        """
        Conecta al usuario al sistema de mensajeria.
        Proceso interno:
          1. Buscar un puerto libre
          2. Crear socket de escucha en ese puerto
          3. Arrancar hilo receptor
          4. Enviar solicitud de conexion al servidor con nombre y puerto
          5. Recibir resultado
        Protocolo:
          1. Conectar al servidor
          2. Enviar "CONNECT\0"
          3. Enviar nombre de usuario\0
          4. Enviar puerto de escucha como cadena\0
          5. Recibir 1 byte: 0=OK, 1=no existe, 2=ya conectado, 3=error
          6. Cerrar conexion
        """
        try:
            # Paso 1: buscar puerto libre
            listen_port = client._find_free_port()

            # Paso 2: crear socket de escucha
            listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listen_socket.bind(('', listen_port))
            listen_socket.listen(10)

            # Paso 3: lanzar hilo receptor ANTES de enviar la solicitud
            client._stop_event.clear()
            t = threading.Thread(
                target=client._listener_thread,
                args=(listen_socket,),
                daemon=True
            )
            t.start()

            # Paso 4: enviar solicitud de conexion al servidor
            sock = client._connect_to_server()
            client._send_string(sock, "CONNECT")
            client._send_string(sock, user)
            client._send_string(sock, str(listen_port))
            result = client._recv_byte(sock)
            sock.close()

            if result == 0:
                # Guardar estado de conexion
                client._listen_port   = listen_port
                client._listen_thread = t
                client._listen_socket = listen_socket
                client._connected_user = user
                print("c> CONNECT OK")
                return client.RC.OK
            elif result == 1:
                # El usuario no existe: parar el hilo
                client._stop_event.set()
                print("c> CONNECT FAIL, USER DOES NOT EXIST")
                return client.RC.USER_ERROR
            elif result == 2:
                client._stop_event.set()
                print("c> USER ALREADY CONNECTED")
                return client.RC.USER_ERROR
            else:
                client._stop_event.set()
                print("c> CONNECT FAIL")
                return client.RC.ERROR

        except Exception as e:
            client._stop_event.set()
            print("c> CONNECT FAIL")
            return client.RC.ERROR

    @staticmethod
    def disconnect(user):
        """
        Desconecta al usuario del sistema.
        Detiene el hilo receptor independientemente del resultado del servidor.
        Protocolo:
          1. Conectar al servidor
          2. Enviar "DISCONNECT\0"
          3. Enviar nombre de usuario\0
          4. Recibir 1 byte: 0=OK, 1=no existe, 2=no conectado, 3=error
          5. Cerrar conexion
        """
        sock = None
        try:
            sock = client._connect_to_server()
            client._send_string(sock, "DISCONNECT")
            client._send_string(sock, user)
            result = client._recv_byte(sock)

            # Segun el enunciado: parar el hilo siempre, incluso en error
            client._stop_event.set()
            client._connected_user = None
            client._listen_port    = None

            if result == 0:
                print("c> DISCONNECT OK")
                return client.RC.OK
            elif result == 1:
                print("c> DISCONNECT FAIL, USER DOES NOT EXIST")
                return client.RC.USER_ERROR
            elif result == 2:
                print("c> DISCONNECT FAIL, USER NOT CONNECTED")
                return client.RC.USER_ERROR
            else:
                print("c> DISCONNECT FAIL")
                return client.RC.ERROR

        except Exception as e:
            # Aunque falle, paramos el hilo (comportamiento especificado en el enunciado)
            client._stop_event.set()
            client._connected_user = None
            client._listen_port    = None
            print("c> DISCONNECT FAIL")
            return client.RC.ERROR
        finally:
            # Cerrar siempre el socket, incluso si hay una excepcion de red
            if sock is not None:
                sock.close()

    @staticmethod
    def users():
        """
        Solicita la lista de usuarios conectados al servidor.
        Solo funciona si el usuario que llama esta conectado.
        Protocolo:
          1. Conectar al servidor
          2. Enviar "USERS\0"
          3. Enviar nombre del usuario que hace la peticion\0
          4. Recibir 1 byte: 0=OK, 1=no conectado, 2=error
          5. Si OK: recibir cadena con numero de usuarios
          6. Recibir tantas cadenas como usuarios (una por usuario)
          7. Cerrar conexion
        """
        sock = None
        try:
            sock = client._connect_to_server()
            client._send_string(sock, "USERS")
            # El enunciado requiere enviar el nombre del usuario que realiza la peticion
            client._send_string(sock, client._connected_user if client._connected_user else "")
            result = client._recv_byte(sock)

            if result == 0:
                count_str = client._recv_string(sock)
                count = int(count_str)
                users_list = []
                for _ in range(count):
                    u = client._recv_string(sock)
                    users_list.append(u)

                print(f"c> CONNECTED USERS ({count} users connected) OK")
                for u in users_list:
                    print(f"   {u}")
                return client.RC.OK

            elif result == 1:
                print("c> CONNECTED USERS FAIL, USER IS NOT CONNECTED")
                return client.RC.USER_ERROR
            else:
                print("c> CONNECTED USERS FAIL")
                return client.RC.ERROR

        except Exception as e:
            print("c> CONNECTED USERS FAIL")
            return client.RC.ERROR
        finally:
            # Cerrar siempre el socket, incluso si hay una excepcion de red
            if sock is not None:
                sock.close()

    @staticmethod
    def send(user, message):
        """
        Envia un mensaje de texto a otro usuario.
        El servidor almacena el mensaje si el destinatario no esta conectado
        y lo entregara cuando se conecte.
        Protocolo:
          1. Conectar al servidor
          2. Enviar "SEND\0"
          3. Enviar nombre del remitente\0
          4. Enviar nombre del destinatario\0
          5. Enviar mensaje\0 (max 255 chars)
          6. Recibir 1 byte: 0=OK (+ id como cadena), 1=no existe, 2=error
          7. Cerrar conexion
        """
        sock = None
        try:
            # El mensaje tiene un maximo de 255 caracteres (256 con '\0')
            if len(message) > 255:
                message = message[:255]

            sock = client._connect_to_server()
            client._send_string(sock, "SEND")
            client._send_string(sock, client._connected_user if client._connected_user else "")
            client._send_string(sock, user)

            # Normalizar espacios del mensaje a traves del servicio web
            message = call_normalize(message)
            client._send_string(sock, message)

            result = client._recv_byte(sock)

            if result == 0:
                msg_id = client._recv_string(sock)
                print(f"c> SEND OK - MESSAGE {msg_id}")
                return client.RC.OK
            elif result == 1:
                print("c> SEND FAIL, USER DOES NOT EXIST")
                return client.RC.USER_ERROR
            else:
                print("c> SEND FAIL")
                return client.RC.ERROR

        except Exception as e:
            print("c> SEND FAIL")
            return client.RC.ERROR
        finally:
            # Cerrar siempre el socket, incluso si hay una excepcion de red
            if sock is not None:
                sock.close()

    @staticmethod
    def sendAttach(user, file, message):
        """
        Envia un mensaje con fichero adjunto a otro usuario (Parte 2).
        Protocolo SENDATTACH cliente->servidor:
          1. Conectar al servidor
          2. Enviar "SENDATTACH\0"
          3. Enviar nombre del remitente\0
          4. Enviar nombre del destinatario\0
          5. Enviar mensaje\0 (max 255 chars)
          6. Enviar nombre del fichero\0 (max 255 chars)
          7. Recibir 1 byte resultado + id si OK
          8. Cerrar conexion
        """
        sock = None
        try:
            if len(message) > 255:
                message = message[:255]

            sock = client._connect_to_server()
            client._send_string(sock, "SENDATTACH")
            client._send_string(sock, client._connected_user if client._connected_user else "")
            client._send_string(sock, user)

            # Normalizar espacios del mensaje a traves del servicio web
            message = call_normalize(message)
            client._send_string(sock, message)

            client._send_string(sock, file)
            result = client._recv_byte(sock)

            if result == 0:
                msg_id = client._recv_string(sock)
                print(f"c> SENDATTACH OK - MESSAGE {msg_id}")
                return client.RC.OK
            elif result == 1:
                print("c> SENDATTACH FAIL, USER DOES NOT EXIST")
                return client.RC.USER_ERROR
            else:
                print("c> SENDATTACH FAIL")
                return client.RC.ERROR

        except Exception as e:
            print("c> SENDATTACH FAIL")
            return client.RC.ERROR
        finally:
            # Cerrar siempre el socket, incluso si hay una excepcion de red
            if sock is not None:
                sock.close()

    # ==================== SHELL INTERACTIVA ====================

    @staticmethod
    def shell():
        """
        Bucle principal de la interfaz de usuario por consola.
        Lee comandos y los delega a los metodos del protocolo.
        """
        while True:
            try:
                command = input("c> ")
                line = command.split(" ")
                if len(line) == 0:
                    continue

                line[0] = line[0].upper()

                if line[0] == "REGISTER":
                    if len(line) == 2:
                        client.register(line[1])
                    else:
                        print("Syntax error. Usage: REGISTER <userName>")

                elif line[0] == "UNREGISTER":
                    if len(line) == 2:
                        client.unregister(line[1])
                    else:
                        print("Syntax error. Usage: UNREGISTER <userName>")

                elif line[0] == "CONNECT":
                    if len(line) == 2:
                        client.connect(line[1])
                    else:
                        print("Syntax error. Usage: CONNECT <userName>")

                elif line[0] == "DISCONNECT":
                    if len(line) == 2:
                        client.disconnect(line[1])
                    else:
                        print("Syntax error. Usage: DISCONNECT <userName>")

                elif line[0] == "USERS":
                    if len(line) == 1:
                        client.users()
                    else:
                        print("Syntax error. Usage: USERS")

                elif line[0] == "SEND":
                    if len(line) >= 3:
                        message = ' '.join(line[2:])
                        client.send(line[1], message)
                    else:
                        print("Syntax error. Usage: SEND <userName> <message>")

                elif line[0] == "SENDATTACH":
                    if len(line) >= 4:
                        message = ' '.join(line[3:])
                        client.sendAttach(line[1], line[2], message)
                    else:
                        print("Syntax error. Usage: SENDATTACH <userName> <filename> <message>")

                elif line[0] == "QUIT":
                    if len(line) == 1:
                        break
                    else:
                        print("Syntax error. Use: QUIT")

                else:
                    print("Error: command " + line[0] + " not valid.")

            except EOFError:
                break
            except Exception as e:
                print("Exception: " + str(e))

    # ==================== ARGUMENTOS Y MAIN ====================

    @staticmethod
    def usage():
        print("Usage: python3 client.py -s <server> -p <port>")

    @staticmethod
    def parseArguments(argv):
        """
        Parsea los argumentos de linea de comandos:
          -s <IP>    IP o hostname del servidor
          -p <port>  Puerto del servidor (1024-65535)
        """
        parser = argparse.ArgumentParser()
        parser.add_argument('-s', type=str, required=True, help='Server IP')
        parser.add_argument('-p', type=int, required=True, help='Server Port')
        args = parser.parse_args()

        if args.s is None:
            parser.error("Usage: python3 client.py -s <server> -p <port>")
            return False

        if args.p < 1024 or args.p > 65535:
            parser.error("Error: Port must be in the range 1024 <= port <= 65535")
            return False

        client._server = args.s
        client._port   = args.p
        return True

    @staticmethod
    def main(argv):
        """
        Punto de entrada del programa cliente.
        Parsea argumentos, lanza la shell interactiva y termina.
        """
        if not client.parseArguments(argv):
            client.usage()
            return

        client.shell()
        print("+++ FINISHED +++")


# ==================== ENTRY POINT ====================

if __name__ == "__main__":
    client.main(sys.argv[1:])
