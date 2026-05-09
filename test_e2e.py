"""
test_e2e.py - Prueba de integración end-to-end del sistema de mensajería distribuido
Sistemas Distribuidos - UC3M - Curso 2025-2026

=============================================================================
 Guía de Pruebas y Evaluación del Sistema (SSDD)
=============================================================================

Este script automatiza el siguiente guion de pruebas paso a paso, diseñado
para evaluar TODAS las funcionalidades del sistema (Parte 1, Parte 2, RPC
y Web Service) cubriendo tanto los casos de éxito (Happy Path) como los
casos límite (Edge Cases).

 1. Preparación del Entorno
    El script arranca automáticamente los 5 procesos necesarios:
      - logger_server      (RPC)
      - server -p 8888     (LOG_RPC_IP=127.0.0.1)
      - web_service.py     (normalización HTTP)
      - client.py x2       (usuario 'dani' y usuario 'iñigo')

 2. Batería de Pruebas

    PRUEBA A: Registro, Conexión y Edge Cases
      1.  REGISTER dani           -> OK
      2.  REGISTER dani           -> USERNAME ALREADY IN USE
      3.  CONNECT fantasma        -> CONNECT FAIL, USER DOES NOT EXIST   [NUEVO]
      4.  CONNECT dani            -> OK
      5.  CONNECT dani            -> USER ALREADY CONNECTED
      6.  REGISTER iñigo          -> OK
      7.  CONNECT iñigo           -> OK
      8.  UNREGISTER fantasma     -> USER DOES NOT EXIST                  [NUEVO]
      9.  SEND usuario_inexistente -> USER DOES NOT EXIST                 [NUEVO]

    PRUEBA B: Lista de Usuarios Conectados (USERS)              [NUEVO]
      10. USERS (ambos conectados) -> lista con dani e iñigo
      11. USERS sin estar conectado -> CONNECTED USERS FAIL

    PRUEBA C: Microservicio Web (Normalización de Espacios)
      12. [dani]  SEND iñigo "Hola      prueba      de      espacios"
      13. [iñigo] Recibe "Hola prueba de espacios" (normalizado)

    PRUEBA D: Mensajería Diferida (Buzón Offline)
      14. [iñigo] DISCONNECT iñigo
      15. [iñigo] DISCONNECT iñigo (ya desconectado) -> FAIL             [NUEVO]
      16. [dani]  SEND iñigo (offline) -> SEND OK, queda en buzón
      17. [iñigo] CONNECT iñigo -> recibe el mensaje almacenado

    PRUEBA E: SENDATTACH con destinatario offline (Buzón + Adjunto) [NUEVO]
      18. [iñigo] DISCONNECT iñigo
      19. [dani]  SENDATTACH iñigo server.c "te paso el codigo" -> OK
      20. [iñigo] CONNECT iñigo -> recibe SEND_MESSAGE_ATTACH pendiente

    PRUEBA F: Transferencia P2P de Ficheros (Parte 2)
      21. [dani]  SENDATTACH iñigo server.c "te paso el codigo" -> OK
      22. [iñigo] GETFILE dani archivo_inventado.txt -> FAIL, FILE NOT FOUND
      23. [iñigo] GETFILE dani server.c -> OK, fichero guardado

    PRUEBA G: Fallback del Web Service caído                    [NUEVO]
      24. [sistema] Detener web_service.py
      25. [dani]    SEND iñigo "mensaje sin web" -> SEND OK (no crash)
      26. [iñigo]   Recibe el mensaje (aunque sin normalizar)
      27. [sistema] Reiniciar web_service.py

    PRUEBA H: Borrado de Usuarios (Unregister)
      28. [dani]  DISCONNECT dani -> OK
      29. [dani]  UNREGISTER dani -> OK
      30. [iñigo] SEND dani "estas?" -> USER DOES NOT EXIST

Ejecución: python3 test_e2e.py  (desde el directorio del proyecto)
"""

import subprocess
import threading
import time
import os
import sys

# ─────────────────────────── Configuración ───────────────────────────────────

PROJECT_DIR  = os.path.dirname(os.path.abspath(__file__))
SERVER_PORT  = 8888
STARTUP_WAIT = 2      # segundos para que los servicios arranquen
CMD_WAIT     = 1.5    # segundos entre comandos al cliente
DELIVER_WAIT = 3.0    # segundos para que se entregue un mensaje

# ─────────────────────────── Helpers ─────────────────────────────────────────

# Contadores globales de resultados
_pass_count = 0
_fail_count = 0


def log(msg):
    """Imprime un mensaje de log con prefijo de test."""
    print(f"[TEST] {msg}", flush=True)


def check(test_name, lines_list, expected_substr, should_exist=True):
    """
    Verifica si alguna línea de la salida contiene (o no) el substring esperado.
    Incrementa los contadores globales de PASS/FAIL y muestra las últimas
    líneas capturadas en caso de fallo para facilitar el diagnóstico.
    """
    global _pass_count, _fail_count
    full_output = "\n".join(lines_list)
    found = expected_substr in full_output

    if should_exist and found:
        log(f"  ✅ PASS: {test_name}")
        _pass_count += 1
        return True
    elif not should_exist and not found:
        log(f"  ✅ PASS: {test_name}")
        _pass_count += 1
        return True
    else:
        action = "encontrar" if should_exist else "NO encontrar"
        log(f"  ❌ FAIL: {test_name} — Se esperaba {action}: '{expected_substr}'")
        recent = lines_list[-8:] if len(lines_list) > 8 else lines_list
        for line in recent:
            log(f"         | {line}")
        _fail_count += 1
        return False


def kill_all(processes):
    """Termina todos los procesos de la lista de forma ordenada."""
    for name, proc in processes:
        if proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=3)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
            log(f"Proceso '{name}' terminado (pid={proc.pid})")


def reader_thread(proc, lines_list):
    """
    Hilo que lee stdout de un proceso línea a línea y acumula en lines_list.
    Se ejecuta en daemon thread para no bloquear la salida del test.
    """
    try:
        for line in iter(proc.stdout.readline, ''):
            lines_list.append(line.rstrip('\n'))
    except Exception:
        pass


def send_cmd(proc, cmd):
    """Envía un comando al stdin del proceso cliente."""
    proc.stdin.write(cmd + "\n")
    proc.stdin.flush()


def clear_output(lines_list):
    """Vacía la lista de salida para verificar sólo las líneas nuevas."""
    lines_list.clear()


# ─────────────────────────── Main ────────────────────────────────────────────

def main():
    processes = []   # lista de (nombre, Popen) para limpiar al final
    web_proc  = None # referencia extra para poder reiniciar el web service

    try:
        # =====================================================================
        #  1. PREPARACIÓN DEL ENTORNO — Levantar todos los servicios
        # =====================================================================

        # ── logger_server (RPC) ───────────────────────────────────────────
        log("Arrancando logger_server...")
        logger_proc = subprocess.Popen(
            ["./logger_server"],
            cwd=PROJECT_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        processes.append(("logger_server", logger_proc))
        rpc_lines = []
        t_rpc = threading.Thread(target=reader_thread, args=(logger_proc, rpc_lines), daemon=True)
        t_rpc.start()

        # ── server -p 8888 ────────────────────────────────────────────────
        log("Arrancando server -p 8888 (LOG_RPC_IP=127.0.0.1)...")
        env = os.environ.copy()
        env["LOG_RPC_IP"] = "127.0.0.1"
        server_proc = subprocess.Popen(
            ["./server", "-p", str(SERVER_PORT)],
            cwd=PROJECT_DIR,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        processes.append(("server", server_proc))
        srv_lines = []
        t_srv = threading.Thread(target=reader_thread, args=(server_proc, srv_lines), daemon=True)
        t_srv.start()

        # ── web_service.py ────────────────────────────────────────────────
        log("Arrancando web_service.py...")
        web_proc = subprocess.Popen(
            ["python3", "web_service.py"],
            cwd=PROJECT_DIR,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        processes.append(("web_service", web_proc))

        log(f"Esperando {STARTUP_WAIT}s para que los servicios arranquen...")
        time.sleep(STARTUP_WAIT)

        # Comprobar que los tres procesos siguen vivos
        for name, proc in processes:
            if proc.poll() is not None:
                log(f"ERROR: El proceso '{name}' murió antes de tiempo (rc={proc.returncode})")
                sys.exit(1)
        log("Los tres servicios están en pie ✓")

        # ── Cliente 1 (dani) ──────────────────────────────────────────────
        log("Abriendo client.py para 'dani'...")
        client1 = subprocess.Popen(
            ["python3", "-u", "client.py", "-s", "127.0.0.1", "-p", str(SERVER_PORT)],
            cwd=PROJECT_DIR,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        processes.append(("client1_dani", client1))
        out1 = []
        t1 = threading.Thread(target=reader_thread, args=(client1, out1), daemon=True)
        t1.start()

        # ── Cliente 2 (iñigo) ─────────────────────────────────────────────
        log("Abriendo client.py para 'iñigo'...")
        client2 = subprocess.Popen(
            ["python3", "-u", "client.py", "-s", "127.0.0.1", "-p", str(SERVER_PORT)],
            cwd=PROJECT_DIR,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        processes.append(("client2_iñigo", client2))
        out2 = []
        t2 = threading.Thread(target=reader_thread, args=(client2, out2), daemon=True)
        t2.start()

        time.sleep(0.5)

        # =====================================================================
        #  PRUEBA A: Registro, Conexión y Edge Cases
        # =====================================================================
        log("═" * 60)
        log("PRUEBA A: Registro, Conexión y Edge Cases")
        log("═" * 60)

        # 1. REGISTER dani -> OK
        clear_output(out1)
        send_cmd(client1, "REGISTER dani")
        time.sleep(CMD_WAIT)
        check("1.  REGISTER dani → OK", out1, "REGISTER OK")

        # 2. REGISTER dani duplicado -> USERNAME ALREADY IN USE
        clear_output(out1)
        send_cmd(client1, "REGISTER dani")
        time.sleep(CMD_WAIT)
        check("2.  REGISTER dani duplicado → USERNAME ALREADY IN USE", out1, "USERNAME ALREADY IN USE")

        # 3. [NUEVO] CONNECT a usuario no registrado -> USER DOES NOT EXIST
        clear_output(out1)
        send_cmd(client1, "CONNECT fantasma")
        time.sleep(CMD_WAIT)
        check("3.  CONNECT fantasma (no registrado) → USER DOES NOT EXIST", out1, "USER DOES NOT EXIST")

        # 4. CONNECT dani -> OK
        clear_output(out1)
        send_cmd(client1, "CONNECT dani")
        time.sleep(CMD_WAIT)
        check("4.  CONNECT dani → OK", out1, "CONNECT OK")

        # 5. CONNECT dani ya conectado -> USER ALREADY CONNECTED
        clear_output(out1)
        send_cmd(client1, "CONNECT dani")
        time.sleep(CMD_WAIT)
        check("5.  CONNECT dani duplicado → USER ALREADY CONNECTED", out1, "USER ALREADY CONNECTED")

        # 6. REGISTER iñigo -> OK
        clear_output(out2)
        send_cmd(client2, "REGISTER iñigo")
        time.sleep(CMD_WAIT)
        check("6.  REGISTER iñigo → OK", out2, "REGISTER OK")

        # 7. CONNECT iñigo -> OK
        clear_output(out2)
        send_cmd(client2, "CONNECT iñigo")
        time.sleep(CMD_WAIT)
        check("7.  CONNECT iñigo → OK", out2, "CONNECT OK")

        # 8. [NUEVO] UNREGISTER de usuario inexistente -> USER DOES NOT EXIST
        clear_output(out1)
        send_cmd(client1, "UNREGISTER fantasma")
        time.sleep(CMD_WAIT)
        check("8.  UNREGISTER fantasma (no existe) → USER DOES NOT EXIST", out1, "USER DOES NOT EXIST")

        # 9. [NUEVO] SEND a usuario nunca registrado -> USER DOES NOT EXIST
        clear_output(out1)
        send_cmd(client1, "SEND nadie_registrado hola")
        time.sleep(CMD_WAIT)
        check("9.  SEND a usuario nunca registrado → USER DOES NOT EXIST", out1, "USER DOES NOT EXIST")

        # Mostrar logs RPC capturados hasta ahora
        log("  ℹ️  Logs RPC capturados:")
        for line in rpc_lines:
            log(f"     rpc| {line}")

        # =====================================================================
        #  PRUEBA B: Lista de Usuarios Conectados (USERS)  [NUEVO]
        # =====================================================================
        log("")
        log("═" * 60)
        log("PRUEBA B: Lista de Usuarios Conectados (USERS)")
        log("═" * 60)

        # 10. USERS con ambos conectados -> debe listar dani e iñigo
        clear_output(out1)
        send_cmd(client1, "USERS")
        time.sleep(CMD_WAIT)
        check("10. USERS (ambos conectados) → OK", out1, "CONNECTED USERS")
        check("10b. USERS lista incluye 'dani'",  out1, "dani")
        check("10c. USERS lista incluye 'iñigo'", out1, "iñigo")

        # 11. [NUEVO] USERS sin estar conectado (usando client3 temporal)
        # Abrimos un cliente temporal que no registra ni conecta a nadie
        log("  Abriendo cliente temporal para probar USERS sin conectar...")
        client_tmp = subprocess.Popen(
            ["python3", "-u", "client.py", "-s", "127.0.0.1", "-p", str(SERVER_PORT)],
            cwd=PROJECT_DIR,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        out_tmp = []
        t_tmp = threading.Thread(target=reader_thread, args=(client_tmp, out_tmp), daemon=True)
        t_tmp.start()
        time.sleep(0.5)
        # Registrar un usuario pero NO conectarlo
        send_cmd(client_tmp, "REGISTER temporal")
        time.sleep(CMD_WAIT)
        send_cmd(client_tmp, "USERS")   # intento de USERS sin CONNECT previo
        time.sleep(CMD_WAIT)
        check("11. USERS sin estar conectado → FAIL", out_tmp, "FAIL")
        # Cerrar el cliente temporal limpiamente
        try:
            send_cmd(client_tmp, "UNREGISTER temporal")
            time.sleep(CMD_WAIT)
            send_cmd(client_tmp, "QUIT")
        except Exception:
            pass
        try:
            client_tmp.stdin.close()
        except Exception:
            pass
        t_tmp.join(timeout=3)
        if client_tmp.poll() is None:
            client_tmp.terminate()

        # =====================================================================
        #  PRUEBA C: Microservicio Web (Normalización de Espacios)
        # =====================================================================
        log("")
        log("═" * 60)
        log("PRUEBA C: Microservicio Web (Normalización de Espacios)")
        log("═" * 60)

        # 12. dani envía mensaje con espacios repetidos
        clear_output(out1)
        clear_output(out2)
        send_cmd(client1, 'SEND iñigo Hola      prueba      de      espacios')
        time.sleep(DELIVER_WAIT)
        check("12. dani SEND → SEND OK", out1, "SEND OK")

        # 13. iñigo recibe el mensaje normalizado
        check("13. iñigo recibe mensaje normalizado", out2, "Hola prueba de espacios")

        # =====================================================================
        #  PRUEBA D: Mensajería Diferida (Buzón Offline)
        # =====================================================================
        log("")
        log("═" * 60)
        log("PRUEBA D: Mensajería Diferida (Buzón Offline)")
        log("═" * 60)

        # 14. iñigo se desconecta
        clear_output(out2)
        send_cmd(client2, "DISCONNECT iñigo")
        time.sleep(CMD_WAIT)
        check("14. DISCONNECT iñigo → OK", out2, "DISCONNECT OK")

        # 15. [NUEVO] DISCONNECT de usuario ya desconectado -> FAIL
        clear_output(out2)
        send_cmd(client2, "DISCONNECT iñigo")
        time.sleep(CMD_WAIT)
        check("15. DISCONNECT iñigo (ya desconectado) → FAIL", out2, "FAIL")

        # 16. dani envía mensaje a iñigo offline -> queda en buzón
        clear_output(out1)
        send_cmd(client1, 'SEND iñigo este mensaje te espera en el buzon')
        time.sleep(CMD_WAIT)
        check("16. dani SEND a iñigo offline → SEND OK", out1, "SEND OK")

        # 17. iñigo reconecta -> recibe automáticamente el mensaje del buzón
        clear_output(out2)
        send_cmd(client2, "CONNECT iñigo")
        time.sleep(DELIVER_WAIT)
        check("17a. CONNECT iñigo → OK", out2, "CONNECT OK")
        check("17b. iñigo recibe mensaje del buzón", out2, "este mensaje te espera en el buzon")

        # =====================================================================
        #  PRUEBA E: SENDATTACH con destinatario offline (Buzón + Adjunto) [NUEVO]
        # =====================================================================
        log("")
        log("═" * 60)
        log("PRUEBA E: SENDATTACH con destinatario offline (Buzón con Adjunto)")
        log("═" * 60)

        # 18. iñigo se desconecta
        clear_output(out2)
        send_cmd(client2, "DISCONNECT iñigo")
        time.sleep(CMD_WAIT)
        check("18. DISCONNECT iñigo → OK (para prueba SENDATTACH offline)", out2, "DISCONNECT OK")

        # 19. dani envía SENDATTACH a iñigo offline -> debe encolarse
        clear_output(out1)
        send_cmd(client1, 'SENDATTACH iñigo server.c mensaje con adjunto offline')
        time.sleep(CMD_WAIT)
        check("19. dani SENDATTACH a iñigo offline → SENDATTACH OK", out1, "SENDATTACH OK")

        # 20. iñigo reconecta -> debe recibir el SEND_MESSAGE_ATTACH pendiente
        clear_output(out2)
        send_cmd(client2, "CONNECT iñigo")
        time.sleep(DELIVER_WAIT)
        check("20a. CONNECT iñigo → OK", out2, "CONNECT OK")
        check("20b. iñigo recibe adjunto pendiente del buzón", out2, "FILE server.c")

        # =====================================================================
        #  PRUEBA F: Transferencia P2P de Ficheros (Parte 2)
        # =====================================================================
        log("")
        log("═" * 60)
        log("PRUEBA F: Transferencia P2P de Ficheros (Parte 2)")
        log("═" * 60)

        # 21. dani envía SENDATTACH a iñigo (online) con server.c
        clear_output(out1)
        clear_output(out2)
        send_cmd(client1, 'SENDATTACH iñigo server.c te paso el codigo')
        time.sleep(DELIVER_WAIT)
        check("21. dani SENDATTACH (iñigo online) → SENDATTACH OK", out1, "SENDATTACH OK")

        # 22. iñigo intenta GETFILE de un archivo que NO existe -> FAIL
        clear_output(out2)
        send_cmd(client2, "GETFILE dani archivo_inventado.txt")
        time.sleep(CMD_WAIT)
        check("22. GETFILE archivo inexistente → GETFILE FAIL", out2, "GETFILE FAIL")

        # 23. iñigo solicita GETFILE de server.c (existe en directorio de dani)
        clear_output(out2)
        send_cmd(client2, "GETFILE dani server.c")
        time.sleep(DELIVER_WAIT)
        check("23. GETFILE server.c → OK", out2, "GETFILE server.c OK")

        # =====================================================================
        #  PRUEBA G: Fallback del Web Service caído  [NUEVO]
        # =====================================================================
        log("")
        log("═" * 60)
        log("PRUEBA G: Fallback del Web Service caído")
        log("═" * 60)

        # Detener el web_service para simular que está caído
        log("  Deteniendo web_service.py para simular caída...")
        if web_proc and web_proc.poll() is None:
            web_proc.terminate()
            try:
                web_proc.wait(timeout=3)
            except Exception:
                web_proc.kill()
        time.sleep(0.5)

        # 24+25. dani envía mensaje con web service caído -> debe llegar igualmente
        clear_output(out1)
        clear_output(out2)
        send_cmd(client1, 'SEND iñigo mensaje sin web service activo')
        time.sleep(DELIVER_WAIT)
        check("24. SEND con web service caído → SEND OK (no crash)", out1, "SEND OK")
        check("25. iñigo recibe el mensaje aunque web service esté caído",
              out2, "mensaje sin web service activo")

        # 26. Reiniciar el web_service para dejar el sistema en buen estado
        log("  Reiniciando web_service.py...")
        web_proc = subprocess.Popen(
            ["python3", "web_service.py"],
            cwd=PROJECT_DIR,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        # Actualizar la lista de procesos para que kill_all lo limpie también
        processes = [(n, p) for n, p in processes if n != "web_service"]
        processes.append(("web_service", web_proc))
        time.sleep(1.0)
        log("  web_service.py reiniciado ✓")

        # =====================================================================
        #  PRUEBA H: Borrado de Usuarios (Unregister)
        # =====================================================================
        log("")
        log("═" * 60)
        log("PRUEBA H: Borrado de Usuarios (Unregister)")
        log("═" * 60)

        # 27. dani se desconecta
        clear_output(out1)
        send_cmd(client1, "DISCONNECT dani")
        time.sleep(CMD_WAIT)
        check("27. DISCONNECT dani → OK", out1, "DISCONNECT OK")

        # 28. dani se da de baja
        clear_output(out1)
        send_cmd(client1, "UNREGISTER dani")
        time.sleep(CMD_WAIT)
        check("28. UNREGISTER dani → OK", out1, "UNREGISTER OK")

        # 29. iñigo intenta enviar a dani (ya no existe)
        clear_output(out2)
        send_cmd(client2, 'SEND dani estas?')
        time.sleep(CMD_WAIT)
        check("29. SEND a usuario eliminado → USER DOES NOT EXIST", out2, "USER DOES NOT EXIST")

        # =====================================================================
        #  RESUMEN FINAL DE RESULTADOS
        # =====================================================================
        log("")
        log("═" * 60)
        log("RESUMEN DE RESULTADOS")
        log("═" * 60)
        total = _pass_count + _fail_count
        log(f"  Total: {total} pruebas | ✅ {_pass_count} PASS | ❌ {_fail_count} FAIL")
        if _fail_count == 0:
            log("  🎉 TODAS LAS PRUEBAS SUPERADAS — Sistema completamente funcional")
        else:
            log(f"  ⚠️  {_fail_count} prueba(s) fallida(s) — Revisar diagnósticos arriba")
        log("═" * 60)

        # ── Cerrar clientes de forma limpia ───────────────────────────────
        log("")
        log("Enviando QUIT a los clientes...")
        try:
            send_cmd(client1, "QUIT")
        except Exception:
            pass
        try:
            send_cmd(client2, "QUIT")
        except Exception:
            pass

        time.sleep(1.0)

        try:
            client1.stdin.close()
        except Exception:
            pass
        try:
            client2.stdin.close()
        except Exception:
            pass

        t1.join(timeout=3)
        t2.join(timeout=3)

    except KeyboardInterrupt:
        log("Prueba interrumpida por el usuario.")

    finally:
        log("Limpiando procesos...")
        kill_all(processes)
        log("Todos los procesos han sido terminados.")

    return 0 if _fail_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
