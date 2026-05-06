"""
test_e2e.py - Prueba de integración end-to-end del sistema de mensajería distribuido
Sistemas Distribuidos - UC3M - Curso 2025-2026

Verifica que:
  1. Los tres servicios arrancan correctamente (logger_server, server, web_service.py)
  2. Dos clientes pueden registrarse y conectarse
  3. Un mensaje con espacios repetidos llega normalizado al destinatario
     gracias a la integración con el servicio web (Parte 2b)

Ejecución: python3 test_e2e.py   (desde el directorio del proyecto)
"""

import subprocess
import threading
import time
import os
import sys

# ─────────────────────────── Configuración ───────────────────────────────────

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
SERVER_PORT = 8888
STARTUP_WAIT = 2      # segundos para que los servicios arranquen
CMD_WAIT    = 1.2     # segundos entre comandos al cliente
OUTPUT_WAIT = 4.0     # segundos para capturar la entrega del mensaje

# ─────────────────────────── Helpers ─────────────────────────────────────────

def log(msg):
    """Imprime un mensaje de log con prefijo de test."""
    print(f"[TEST] {msg}", flush=True)


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


# ─────────────────────────── Main ────────────────────────────────────────────

def main():
    processes = []   # lista de (nombre, Popen) para limpiar al final
    result    = False

    try:
        # ── 1. Levantar logger_server ─────────────────────────────────────
        log("Arrancando logger_server...")
        logger_proc = subprocess.Popen(
            ["./logger_server"],
            cwd=PROJECT_DIR,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        processes.append(("logger_server", logger_proc))

        # ── 2. Levantar server con LOG_RPC_IP ─────────────────────────────
        log("Arrancando server -p 8888 (LOG_RPC_IP=localhost)...")
        env = os.environ.copy()
        env["LOG_RPC_IP"] = "localhost"
        server_proc = subprocess.Popen(
            ["./server", "-p", str(SERVER_PORT)],
            cwd=PROJECT_DIR,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        processes.append(("server", server_proc))

        # ── 3. Levantar web_service.py ────────────────────────────────────
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

        # ── 4. Abrir cliente 1 (usuario1) ─────────────────────────────────
        log("Abriendo client.py para usuario1...")
        client1 = subprocess.Popen(
            ["python3", "-u", "client.py", "-s", "localhost", "-p", str(SERVER_PORT)],
            cwd=PROJECT_DIR,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,   # line-buffered
        )
        processes.append(("client1", client1))

        # Lector no bloqueante para cliente1
        out1_lines = []
        t1 = threading.Thread(target=reader_thread, args=(client1, out1_lines), daemon=True)
        t1.start()

        # ── 5. Abrir cliente 2 (usuario2) ─────────────────────────────────
        log("Abriendo client.py para usuario2...")
        client2 = subprocess.Popen(
            ["python3", "-u", "client.py", "-s", "localhost", "-p", str(SERVER_PORT)],
            cwd=PROJECT_DIR,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        processes.append(("client2", client2))

        # Lector no bloqueante para cliente2
        out2_lines = []
        t2 = threading.Thread(target=reader_thread, args=(client2, out2_lines), daemon=True)
        t2.start()

        time.sleep(0.5)

        # ── 6. Registrar y conectar usuario2 primero ──────────────────────
        # (así recibirá el mensaje en tiempo real cuando usuario1 lo envíe)
        log("Registrando y conectando usuario2...")
        send_cmd(client2, "REGISTER usuario2")
        time.sleep(CMD_WAIT)
        send_cmd(client2, "CONNECT usuario2")
        time.sleep(CMD_WAIT)

        # ── 7. Registrar y conectar usuario1 ─────────────────────────────
        log("Registrando y conectando usuario1...")
        send_cmd(client1, "REGISTER usuario1")
        time.sleep(CMD_WAIT)
        send_cmd(client1, "CONNECT usuario1")
        time.sleep(CMD_WAIT)

        # ── 8. usuario1 envía mensaje con espacios repetidos ──────────────
        raw_msg = "SEND usuario2 Hola     esto    tiene    espacios"
        log(f"usuario1 envía: '{raw_msg}'")
        send_cmd(client1, raw_msg)

        # Dar tiempo a la entrega y normalización
        log(f"Esperando {OUTPUT_WAIT}s para que el mensaje sea entregado...")
        time.sleep(OUTPUT_WAIT)

        # ── 9. Cerrar clientes de forma limpia ────────────────────────────
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

        # Cerrar pipes para que los hilos lectores terminen
        try:
            client1.stdin.close()
        except Exception:
            pass
        try:
            client2.stdin.close()
        except Exception:
            pass

        # Esperar a que los hilos lectores vacíen el buffer
        t1.join(timeout=3)
        t2.join(timeout=3)

        # ── 10. Verificar resultado ───────────────────────────────────────
        log("─" * 60)
        log("Salida cliente1 (usuario1):")
        for line in out1_lines:
            log(f"  c1| {line}")

        log("─" * 60)
        log("Salida cliente2 (usuario2):")
        for line in out2_lines:
            log(f"  c2| {line}")

        log("─" * 60)

        # El mensaje normalizado esperado (un único espacio entre palabras)
        expected_normalized = "Hola esto tiene espacios"
        out2_full = "\n".join(out2_lines)

        if expected_normalized in out2_full:
            log(f"✅ PRUEBA SUPERADA: El mensaje llegó normalizado → '{expected_normalized}'")
            result = True
        else:
            log(f"❌ PRUEBA FALLIDA: No se encontró '{expected_normalized}' en la salida de usuario2")
            # Diagnóstico adicional: buscar cualquier línea que contenga 'Hola'
            hola_lines = [l for l in out2_lines if "Hola" in l]
            if hola_lines:
                log(f"   El mensaje SÍ llegó, pero sin normalizar: {hola_lines}")
            else:
                log("   El mensaje no llegó en absoluto a usuario2")
            result = False

    except KeyboardInterrupt:
        log("Prueba interrumpida por el usuario.")

    finally:
        # ── 11. Matar todos los procesos ──────────────────────────────────
        log("Limpiando procesos...")
        kill_all(processes)
        log("Todos los procesos han sido terminados.")

    return 0 if result else 1


if __name__ == "__main__":
    sys.exit(main())
