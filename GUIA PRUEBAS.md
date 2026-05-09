# Guía de Pruebas y Evaluación del Sistema (SSDD)

Este documento contiene un guion paso a paso diseñado para evaluar de forma interactiva y manual todas las funcionalidades del sistema (Parte 1, Parte 2, RPC y Web Service), cubriendo tanto los casos de éxito (Happy Path) como los casos límite (Edge Cases).

## 1. Preparación del Entorno

Para ejecutar esta batería de pruebas, compile primero el proyecto (`make all`), abra **5 terminales** conectadas al entorno y ejecute los servicios en el siguiente orden:

*   **Terminal 1 (RPC):** `./logger_server`
*   **Terminal 2 (Servidor C):** `export LOG_RPC_IP=127.0.0.1 && ./server -p 8888`
*   **Terminal 3 (Web Service):** `python3 web_service.py`
*   **Terminal 4 (Cliente 1):** `python3 client.py -s 127.0.0.1 -p 8888` *(Será el usuario 'dani')*
*   **Terminal 5 (Cliente 2):** `python3 client.py -s 127.0.0.1 -p 8888` *(Será el usuario 'iñigo')*

---

## 2. Batería de Pruebas (Ejecución Paso a Paso)

### PRUEBA A: Registro, Conexión y Control de Estado (Edge Cases)
Verificamos el registro de usuarios, las conexiones y el control de errores básicos.

*En la Terminal 4 (Cliente 1):*
1. `REGISTER dani` -> *Debe devolver OK.*
2. `REGISTER dani` -> *Debe fallar: USERNAME ALREADY IN USE.*
3. `CONNECT fantasma` -> *Debe fallar: USER DOES NOT EXIST.*
4. `CONNECT dani` -> *Debe devolver OK.*
5. `CONNECT dani` -> *Debe fallar: USER ALREADY CONNECTED.*

*En la Terminal 5 (Cliente 2):*
6. `REGISTER iñigo` -> *Debe devolver OK.*
7. `CONNECT iñigo` -> *Debe devolver OK.*

> **Nota:** Verifique la Terminal 1 (RPC). Deberán aparecer los logs de todas estas operaciones.

### PRUEBA B: Lista de Usuarios Conectados (USERS)
Verificamos que el servidor reporta correctamente quién está en línea.

*En la Terminal 4 (Cliente 1):*
8. `USERS` -> *Debe mostrar a 'dani' e 'iñigo' con el formato `usuario :: IP :: puerto`.*

### PRUEBA C: Microservicio Web (Normalización de Espacios)
Verificamos la integración con el servicio REST en Python.

*En la Terminal 4 (Cliente 1):*
9. `SEND iñigo Hola      prueba      de      espacios` 

*En la Terminal 5 (Cliente 2):*
10. *Debe recibir automáticamente:* `MESSAGE X FROM dani` con el texto `Hola prueba de espacios` (El servicio HTTP ha normalizado la cadena eliminando los espacios sobrantes).

### PRUEBA D: Mensajería Diferida (Buzón Offline)
Verificamos qué pasa si enviamos mensajes a alguien que no está conectado.

*En la Terminal 5 (Cliente 2):*
11. `DISCONNECT iñigo` -> *Debe devolver OK. El usuario pasa a estar offline.*
12. `DISCONNECT iñigo` -> *Debe fallar: USER NOT CONNECTED.*

*En la Terminal 4 (Cliente 1):*
13. `SEND iñigo este mensaje te espera en el buzon` -> *El servidor C debe devolver SEND OK y encolarlo.*

*En la Terminal 5 (Cliente 2):*
14. `CONNECT iñigo` -> *Nada más conectar, debe recibir automáticamente el mensaje almacenado.*

### PRUEBA E: Transferencia P2P y Errores de Fichero (Parte 2)
Verificamos el envío de adjuntos y la conexión directa cliente-cliente.

*En la Terminal 4 (Cliente 1):*
15. `SENDATTACH iñigo server.c te paso el codigo` -> *Envía la notificación con el fichero adjunto.*

*En la Terminal 5 (Cliente 2):*
16. *Debe recibir la notificación de SENDATTACH.*
17. `GETFILE dani archivo_inventado.txt` -> *Debe fallar sin colgar el cliente: GETFILE FAIL, FILE NOT FOUND.*
18. `GETFILE dani server.c` -> *Debe devolver GETFILE server.c OK y guardar el archivo en el directorio actual.*

### PRUEBA F: Fallback del Web Service Caído
Verificamos el diseño defensivo del sistema si el microservicio falla.

*En la Terminal 3 (Web Service):*
19. *Pulse `Ctrl+C` para detener el servidor web.*

*En la Terminal 4 (Cliente 1):*
20. `SEND iñigo este mensaje no se normalizara` -> *Debe devolver SEND OK sin que el cliente se cuelgue.*

*En la Terminal 5 (Cliente 2):*
21. *Debe recibir el mensaje.*

*(Puede volver a arrancar el Web Service en la Terminal 3 si lo desea).*

### PRUEBA G: Borrado de Usuarios (Unregister)
Verificamos el borrado definitivo y sus consecuencias.

*En la Terminal 4 (Cliente 1):*
22. `DISCONNECT dani` -> *Debe devolver OK.*
23. `UNREGISTER dani` -> *Elimina a 'dani' del sistema. Debe devolver OK.*

*En la Terminal 5 (Cliente 2):*
24. `SEND dani estas?` -> *Debe fallar: SEND FAIL, USER DOES NOT EXIST.*

---
**Fin de las pruebas.** Si el sistema supera este guion, todos los requisitos de concurrencia, sincronización, sockets, RPC y HTTP están funcionando correctamente, validando el 100% de la lógica requerida.
