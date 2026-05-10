"""
test_concurrency.py - Prueba de concurrencia del sistema de mensajería distribuido
Sistemas Distribuidos - UC3M - Curso 2025-2026

Verifica el comportamiento del servidor bajo carga concurrente:
  - 4 clientes registrándose y conectándose en paralelo
  - Todos enviando mensajes entre sí simultáneamente (12 mensajes en paralelo)
  - Verificación de que ningún mensaje se pierde ni el servidor colapsa

Ejecución: python3 test_concurrency.py  (desde el directorio del proyecto)
"""

import subprocess
import threading
import time
import os
import sys

PROJECT_DIR  = os.path.dirname(os.path.abspath(__file__))
SERVER_PORT  = 8888
STARTUP_WAIT = 2
CMD_WAIT     = 1.5
DELIVER_WAIT = 5.0   # más margen para entrega concurrente

_pass_count = 0
_fail_count = 0


def log(msg):
    print(f"[CONCURRENCY] {msg}", flush=True)


def check(test_name, lines_list, expected_substr, should_exist=True):
    global _pass_count, _fail_count
    full_output = "\n".join(lines_list)
    found = expected_substr in full_output

    if (should_exist and found) or (not should_exist and not found):
        log(f"  [PASS] {test_name}")
        _pass_count += 1
        return True
    else:
        action = "encontrar" if should_exist else "NO encontrar"
        log(f"  [FAIL] {test_name} — Se esperaba {action}: '{expected_substr}'")
        recent = lines_list[-6:] if len(lines_list) > 6 else lines_list
        for line in recent:
            log(f"         | {line}")
        _fail_count += 1
        return False


def kill_all(processes):
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
    try:
        for line in iter(proc.stdout.readline, ''):
            lines_list.append(line.rstrip('\n'))
    except Exception:
        pass


def send_cmd(proc, cmd):
    proc.stdin.write(cmd + "\n")
    proc.stdin.flush()


def clear_output(lines_list):
    lines_list.clear()


def main():
    processes = []

    try:
        # ── Levantar infraestructura ──────────────────────────────────────────
        log("Arrancando logger_server...")
        logger_proc = subprocess.Popen(
            ["./logger_server"], cwd=PROJECT_DIR,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        processes.append(("logger_server", logger_proc))

        log("Arrancando server -p 8888...")
        env = os.environ.copy()
        env["LOG_RPC_IP"] = "127.0.0.1"
        server_proc = subprocess.Popen(
            ["./server", "-p", str(SERVER_PORT)], cwd=PROJECT_DIR, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1,
        )
        processes.append(("server", server_proc))
        srv_lines = []
        threading.Thread(target=reader_thread, args=(server_proc, srv_lines), daemon=True).start()

        log("Arrancando web_service.py...")
        web_proc = subprocess.Popen(
            ["python3", "web_service.py"], cwd=PROJECT_DIR,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        processes.append(("web_service", web_proc))

        log(f"Esperando {STARTUP_WAIT}s para que los servicios arranquen...")
        time.sleep(STARTUP_WAIT)

        for name, proc in processes:
            if proc.poll() is not None:
                log(f"ERROR: '{name}' murió antes de tiempo. Abortando.")
                sys.exit(1)
        log("Servicios en pie")

        # ── Lanzar 4 clientes ─────────────────────────────────────────────────
        USERS = ["alice", "bob", "carol", "dave"]
        clients = []
        outputs = []

        log("Abriendo 4 clientes simultáneamente...")
        for username in USERS:
            proc = subprocess.Popen(
                ["python3", "-u", "client.py", "-s", "127.0.0.1", "-p", str(SERVER_PORT)],
                cwd=PROJECT_DIR,
                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, text=True, bufsize=1,
            )
            processes.append((f"client_{username}", proc))
            out = []
            threading.Thread(target=reader_thread, args=(proc, out), daemon=True).start()
            clients.append(proc)
            outputs.append(out)

        time.sleep(0.5)

        # =====================================================================
        #  FASE 1: REGISTER y CONNECT en paralelo (todos a la vez)
        # =====================================================================
        log("═" * 60)
        log("FASE 1: REGISTER y CONNECT simultáneos (4 clientes en paralelo)")
        log("═" * 60)

        # Enviar REGISTER a todos a la vez usando hilos
        def register_and_connect(proc, username):
            send_cmd(proc, f"REGISTER {username}")
            time.sleep(0.5)
            send_cmd(proc, f"CONNECT {username}")

        threads = []
        for proc, username in zip(clients, USERS):
            t = threading.Thread(target=register_and_connect, args=(proc, username))
            threads.append(t)

        # Lanzar todos simultáneamente
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        time.sleep(CMD_WAIT * 2)

        # Verificar que todos se registraron y conectaron
        for i, username in enumerate(USERS):
            check(f"REGISTER {username} → OK", outputs[i], "REGISTER OK")
            check(f"CONNECT  {username} → OK", outputs[i], "CONNECT OK")

        # =====================================================================
        #  FASE 2: SEND masivo concurrente (todos envían a todos a la vez)
        # =====================================================================
        log("")
        log("═" * 60)
        log("FASE 2: SEND masivo concurrente (12 mensajes simultáneos)")
        log("═" * 60)
        # 4 clientes × 3 destinatarios cada uno = 12 mensajes concurrentes
        # Cada cliente envía a los otros 3

        for out in outputs:
            out.clear()

        def send_to_all(sender_proc, sender_name, recipient_names):
            for recipient in recipient_names:
                send_cmd(sender_proc, f"SEND {recipient} mensaje_de_{sender_name}")

        send_threads = []
        for i, (proc, username) in enumerate(zip(clients, USERS)):
            recipients = [u for u in USERS if u != username]
            t = threading.Thread(target=send_to_all, args=(proc, username, recipients))
            send_threads.append(t)

        log("Disparando los 12 mensajes simultáneamente...")
        for t in send_threads:
            t.start()
        for t in send_threads:
            t.join()

        log(f"Esperando {DELIVER_WAIT}s para que todos los mensajes se entreguen...")
        time.sleep(DELIVER_WAIT)

        # Verificar que cada cliente recibió exactamente 3 mensajes
        # (uno de cada uno de los otros 3 usuarios)
        log("Verificando recepción de mensajes:")
        all_ok = True
        for i, recipient_name in enumerate(USERS):
            senders = [u for u in USERS if u != recipient_name]
            for sender_name in senders:
                result = check(
                    f"{recipient_name} recibió mensaje de {sender_name}",
                    outputs[i],
                    f"mensaje_de_{sender_name}",
                )
                if not result:
                    all_ok = False

        if all_ok:
            log("  -> Ningun mensaje se perdio bajo carga concurrente")

        # =====================================================================
        #  FASE 3: DISCONNECT y RECONNECT concurrentes
        # =====================================================================
        log("")
        log("═" * 60)
        log("FASE 3: DISCONNECT y RECONNECT concurrentes")
        log("═" * 60)

        for out in outputs:
            out.clear()

        def disconnect_reconnect(proc, username):
            send_cmd(proc, f"DISCONNECT {username}")
            time.sleep(0.3)
            send_cmd(proc, f"CONNECT {username}")

        dr_threads = []
        for proc, username in zip(clients, USERS):
            t = threading.Thread(target=disconnect_reconnect, args=(proc, username))
            dr_threads.append(t)

        log("Lanzando DISCONNECT+CONNECT en paralelo para los 4 clientes...")
        for t in dr_threads:
            t.start()
        for t in dr_threads:
            t.join()

        time.sleep(CMD_WAIT * 2)

        for i, username in enumerate(USERS):
            check(f"DISCONNECT {username} → OK", outputs[i], "DISCONNECT OK")
            check(f"RECONNECT  {username} → OK", outputs[i], "CONNECT OK")

        # =====================================================================
        #  FASE 4: Verificar que el servidor sigue vivo tras la carga
        # =====================================================================
        log("")
        log("═" * 60)
        log("FASE 4: Verificar integridad del servidor tras la carga")
        log("═" * 60)

        global _pass_count, _fail_count
        if server_proc.poll() is not None:
            log(f"  [FAIL] El servidor murio durante las pruebas (rc={server_proc.returncode})")
            _fail_count += 1
        else:
            log("  [PASS] El servidor sigue en pie tras toda la carga concurrente")
            _pass_count += 1

        # USERS final: verificar que los 4 siguen en la lista
        for out in outputs:
            out.clear()
        send_cmd(clients[0], "USERS")
        time.sleep(CMD_WAIT)
        for username in USERS:
            check(f"Servidor lista a '{username}' en USERS tras carga", outputs[0], username)

        # ── RESUMEN ───────────────────────────────────────────────────────────
        log("")
        log("═" * 60)
        log("RESUMEN DE PRUEBAS DE CONCURRENCIA")
        log("═" * 60)
        total = _pass_count + _fail_count
        log(f"  Total: {total} checks | {_pass_count} PASS | {_fail_count} FAIL")
        if _fail_count == 0:
            log("  CONCURRENCIA: Sistema completamente estable bajo carga")
        else:
            log(f"  AVISO: {_fail_count} check(s) fallido(s) — posible problema de concurrencia")
        log("═" * 60)

        # ── Cerrar clientes ───────────────────────────────────────────────────
        for proc in clients:
            try:
                send_cmd(proc, "QUIT")
            except Exception:
                pass
        time.sleep(1.0)
        for proc in clients:
            try:
                proc.stdin.close()
            except Exception:
                pass

    except KeyboardInterrupt:
        log("Prueba interrumpida por el usuario.")

    finally:
        log("Limpiando procesos...")
        kill_all(processes)
        log("Todos los procesos terminados.")

    return 0 if _fail_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
