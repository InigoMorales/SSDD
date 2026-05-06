# SSDD Proyecto - Contexto para el equipo
## Sistemas Distribuidos - UC3M 2025-2026

---

## QUÉ PIDE LA PRÁCTICA

Es un sistema de mensajería distribuido tipo WhatsApp simplificado.
La entrega es el **10 de mayo de 2026 a las 23:55** via Aula Global.
Se entrega un único zip: `ssdd_proyecto_NIA1_NIA2.zip`

### Puntuación
| Parte | Descripción | Puntos |
|---|---|---|
| Parte 1 | Servidor C + Cliente Python con sockets | 6 pts |
| Parte 2a | SENDATTACH (envío de ficheros adjuntos) | 2 pts |
| Parte 2b | Servicio Web (conversor de mensajes) | 1 pt |
| Parte 2c | Servicio RPC (registro de operaciones) | 1 pt |
| **TOTAL** | | **10 pts** |

> ⚠️ Sin memoria aprobada = práctica suspensa automáticamente  
> ⚠️ Sin comentarios en el código = 0  
> ⚠️ Con warnings = penalización  

---

## ARQUITECTURA DEL SISTEMA

```
[Cliente Python] ──sockets TCP──► [Servidor C]
      │                                │
      │                           ──► [Servidor RPC] (logger)
      │
      └──HTTP──► [Servicio Web Python] (normaliza espacios en mensajes)
```

### Componentes
- **Servidor de mensajería** (`server.c`): en C, multihilo, sockets TCP
- **Cliente** (`client.py`): en Python, multihilo (hilo receptor de mensajes)
- **Servicio web** (`web_service.py`): en Python, normaliza espacios en mensajes
- **Servidor RPC** (`logger_server_impl.c` + stubs): en C, ONC-RPC, registra operaciones

### Protocolo
- Todas las cadenas van terminadas en `\0`
- Los códigos de respuesta son 1 byte (0=OK, 1=USER_ERROR, 2+=ERROR)
- Cada operación abre y cierra su propia conexión TCP al servidor
- El cliente tiene un **hilo receptor** escuchando en un puerto libre
  que el servidor usa para entregarle mensajes

---

## QUÉ ESTÁ HECHO

### ✅ client.py (Python)
Operaciones implementadas con protocolo exacto del enunciado:
- `REGISTER` / `UNREGISTER`
- `CONNECT` (abre hilo receptor en puerto libre antes de conectar)
- `DISCONNECT` (para el hilo receptor siempre, incluso si hay error)
- `USERS` (envía nombre del usuario que pide la lista)
- `SEND` (envía mensaje, recibe id, espera ACK por hilo receptor)
- `SENDATTACH` (Parte 2 - esqueletado y funcional)

El hilo receptor maneja:
- `SEND_MESSAGE` → muestra `s> MESSAGE id FROM user`
- `SEND_MESS_ACK` → muestra `c> SEND MESSAGE id OK`
- `SEND_MESSAGE_ATTACH` → muestra mensaje + nombre fichero (Parte 2)
- `SEND_MESS_ATTACH_ACK` → confirmación con fichero (Parte 2)
- `GET_FILE` → sirve ficheros a otros clientes (Parte 2)

### ✅ server.c (C)
- Servidor multihilo: un hilo detached por cada cliente
- Mutex protege la tabla de usuarios (hasta 128 usuarios)
- Mensajes pendientes en lista enlazada por usuario
- Entrega inmediata si el destinatario está conectado
- Si el destinatario se desconecta durante entrega, lo marca como desconectado
- Al reconectarse un usuario, entrega todos sus mensajes pendientes
- `USERS` devuelve formato `usuario :: IP :: puerto` (compatible Parte 2)
- `SENDATTACH` implementado (Parte 2)
- Llama a `rpc_log_operation()` en cada operación (Parte 2)

### ✅ web_service.py (Python)
- Servidor HTTP en Python puro (sin Flask ni dependencias externas)
- `POST /normalize` → elimina espacios repetidos del mensaje
- `GET /health` → health-check
- Función auxiliar `call_normalize()` lista para integrar en `client.py`

### ✅ Servicio RPC (C, ONC-RPC)
- `logger.x` → interfaz definida a mano (justificada en memoria)
- `logger_server_impl.c` → implementación del servidor RPC
- `rpc_client.c / rpc_client.h` → módulo que usa `server.c` para llamar al RPC
- Lee `LOG_RPC_IP` del entorno para localizar el servidor RPC
- Si `LOG_RPC_IP` no está definida, el servidor funciona sin RPC (no es crítico)
- Stubs generados con `rpcgen`: `logger_clnt.c`, `logger_svc.c`, `logger_xdr.c`, `logger.h`

### ✅ Makefile
- `make all` → compila `server` y `logger_server`
- `make clean` → limpia ejecutables y objetos
- Compila con `-Wall -Wextra -g -pthread`
- Los ficheros de rpcgen se compilan sin `-Wextra` (warnings inevitables del generador)
- **Compila con cero warnings**

### ✅ README
- Instrucciones completas de compilación y arranque
- Orden de arranque de todos los procesos
- Resumen del protocolo

---

## QUÉ FALTA ❌

### 1. Integrar el servicio web en client.py
Actualmente `web_service.py` existe y tiene la función `call_normalize()`,
pero `client.py` **no la llama todavía**.

Hay que añadir en `client.py`, en los métodos `send()` y `sendAttach()`,
antes de enviar el mensaje al servidor:

```python
from web_service import call_normalize

# En send() y sendAttach(), antes de send_string(sock, message):
message = call_normalize(message)  # normalizar espacios via servicio web
```

> ⚠️ El servicio web debe estar corriendo en localhost:8080 antes de que
> el cliente envíe mensajes. Si no está corriendo, `call_normalize()` 
> devuelve el mensaje original (fallback seguro).

### 2. autores.txt
Crear el fichero con este formato:
```
Nombre Apellido1 Apellido2   NIA   email@alumnos.uc3m.es
Nombre Apellido1 Apellido2   NIA   email@alumnos.uc3m.es
```

### 3. memoria.pdf
Documento PDF con estas secciones obligatorias:
- Portada (nombres, NIAs, emails)
- Índice de contenidos
- Descripción del código (funciones principales, SIN código fuente)
- Cómo compilar y ejecutar todos los procesos
- Batería de pruebas y resultados (casos normales + casos extremos)
- Conclusiones y problemas encontrados
- Máximo 15 páginas, texto justificado, números de página (menos portada)

> La memoria es **imprescindible aprobarla** para aprobar la práctica.

### 4. Pruebas end-to-end
Verificar que todo funciona junto antes de entregar:
```bash
make all
./logger_server &
export LOG_RPC_IP=localhost && ./server -p 8888 &
python3 web_service.py &
python3 client.py -s localhost -p 8888   # terminal cliente 1
python3 client.py -s localhost -p 8888   # terminal cliente 2
```

---

## CÓMO ARRANCAR TODO

```bash
# Terminal 1 - Servidor RPC
./logger_server

# Terminal 2 - Servidor de mensajería
export LOG_RPC_IP=localhost
./server -p 8888

# Terminal 3 - Servicio web (en la máquina de cada cliente)
python3 web_service.py

# Terminal 4+ - Clientes
python3 client.py -s localhost -p 8888
```

---

## ESTRUCTURA DE FICHEROS

```
ssdd_proyecto_NIA1_NIA2/
├── client.py                ← cliente Python (HECHO)
├── server.c                 ← servidor C (HECHO)
├── rpc_client.c             ← módulo RPC para server.c (HECHO)
├── rpc_client.h             ← cabecera rpc_client (HECHO)
├── logger.x                 ← interfaz ONC-RPC (HECHO)
├── logger.h                 ← generado por rpcgen (HECHO)
├── logger_clnt.c            ← generado por rpcgen (HECHO)
├── logger_xdr.c             ← generado por rpcgen (HECHO)
├── logger_svc.c             ← generado por rpcgen (HECHO)
├── logger_server_impl.c     ← implementación servidor RPC (HECHO)
├── web_service.py           ← servicio web Python (HECHO)
├── Makefile                 ← compilación (HECHO)
├── README                   ← instrucciones (HECHO)
├── autores.txt              ← ❌ FALTA
└── memoria.pdf              ← ❌ FALTA
```
