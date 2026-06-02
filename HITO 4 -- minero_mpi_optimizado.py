#!/usr/bin/env python3
"""
Minero MPI Optimizado SHA-256 — Hito 4 (Corregido y Blindado)
Estructura de Computadores, Paralelismo y Sistemas Distribuidos
"""

import hashlib
import time
import argparse
from mpi4py import MPI

# ──────────────────────────────────────────────
# Bloque de datos oficial
# ──────────────────────────────────────────────
BLOCK_ID  = 1
PREV_HASH = "0" * 64
DATA      = "Proyecto Cluster MPI EUNEIZ"

CHECK_INTERVAL = 10_000

# Tags MPI
TAG_RESULT = 1   # Worker → Master: Envía el resultado exitoso
TAG_DONE   = 2   # Worker → Master: Notifica que terminó su bucle sin éxito
TAG_STOP   = 99  # Master → Worker: Señal con el rank del ganador (o -1 si nadie ganó)


def construir_cabecera(nonce: int) -> str:
    return f"{BLOCK_ID}|{PREV_HASH}|{DATA}|{nonce}"

def calcular_hash(cabecera: str) -> str:
    return hashlib.sha256(cabecera.encode('utf-8')).hexdigest()

def proceso_master(comm, size: int, inicio: float) -> None:
    winner = -1
    done_count = 0

    # 1. Espera centralizada: Escucha tanto éxitos como finalizaciones naturales
    while True:
        status = MPI.Status()
        msg = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        tag = status.Get_tag()
        source = status.Get_source()

        if tag == TAG_RESULT:
            winner = source
            print(f"\n[Master] ¡Solución recibida del proceso {source}!")
            break
        elif tag == TAG_DONE:
            done_count += 1
            if done_count == size - 1:  # Todos los workers agotaron sus bucles
                print(f"\n[Master] Todos los procesos terminaron sin encontrar el nonce.")
                break

    # 2. Enviar el rank del ganador (o -1) a TODOS los workers.
    # Al usar 'send' evitamos perder requests de 'isend' en el recolector de basura.
    for w in range(1, size):
        comm.send(winner, dest=w, tag=TAG_STOP)

    # 3. Limpiar buffer de mensajes residuales (ej. un TAG_DONE tardío cruzado con un TAG_RESULT)
    while comm.Iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG):
        comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG)

    # 4. Notificación y Validación de Parada (bcast) apuntando al worker ganador
    if winner != -1:
        resultado_final = comm.bcast(None, root=winner)
        fin = time.time()
        print(f"[Master] Bcast validado -> Hash {resultado_final['hash']} (Tiempo: {fin - inicio:.4f}s). Cerrando...")

def proceso_worker(comm, rank: int, size: int, dificultad: int, nonce_max: int, inicio: float) -> None:
    objetivo = "0" * dificultad
    intentos = 0
    winner = -1
    resultado_local = None

    for nonce in range(rank, nonce_max + 1, size):
        intentos += 1

        # ── Parte I: Iprobe de alta eficiencia ──
        if intentos % CHECK_INTERVAL == 0:
            if comm.Iprobe(source=0, tag=TAG_STOP):
                winner = comm.recv(source=0, tag=TAG_STOP)
                print(f"[Worker {rank}] Señal de parada detectada vía Iprobe. Abortando...")
                break

        cabecera = construir_cabecera(nonce)
        hash_hex = calcular_hash(cabecera)

        if hash_hex.startswith(objetivo):
            print(f"\n[Worker {rank}] ¡NONCE ENCONTRADO! Avisando al master...")
            resultado_local = {
                "nonce": nonce, "cabecera": cabecera, 
                "hash": hash_hex, "intentos": intentos
            }
            # Avisamos al master
            comm.send(resultado_local, dest=0, tag=TAG_RESULT)
            
            # Bloqueamos esperando la confirmación (resuelve el error de sincronización de parada)
            winner = comm.recv(source=0, tag=TAG_STOP)
            break
    else:
        # El bucle terminó naturalmente sin encontrar el hash (Prevención de Deadlock)
        comm.send(None, dest=0, tag=TAG_DONE)
        winner = comm.recv(source=0, tag=TAG_STOP)

    # ── Parte II: Validación del sistema (bcast) ──
    if winner != -1:
        # Si este worker es el ganador, inyecta el resultado; si no, inyecta None.
        resultado_final = comm.bcast(resultado_local if rank == winner else None, root=winner)
        
        # Validación requerida en el enunciado para los workers
        if rank != winner:
            print(f"[Worker {rank}] Terminado. Validado bcast del root {winner}: {resultado_final['hash'][:10]}...")
    else:
        print(f"[Worker {rank}] Terminado de forma limpia. Intentos: {intentos:,}")


def minar_mpi(dificultad: int, nonce_max: int) -> None:
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if size < 2:
        print("Error: Este script requiere al menos 2 procesos (1 Master, N Workers).")
        return

    if rank == 0:
        print("=" * 60)
        print("   MINERO MPI OPTIMIZADO SHA-256 — HITO 4 (BLINDADO)")
        print("=" * 60)

    comm.Barrier()
    inicio = time.time()

    if rank == 0:
        proceso_master(comm, size, inicio)
    else:
        proceso_worker(comm, rank, size, dificultad, nonce_max, inicio)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dificultad", type=int, default=4)
    parser.add_argument("--max", type=int, default=2_000_000)
    args = parser.parse_args()
    
    if args.dificultad < 1:
        parser.error("La dificultad debe ser >= 1.")
        
    minar_mpi(args.dificultad, args.max)
