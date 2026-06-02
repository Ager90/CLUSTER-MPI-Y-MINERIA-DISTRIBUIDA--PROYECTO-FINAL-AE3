#!/usr/bin/env python3
"""
Minero distribuido SHA-256 — Hito Final (AE3)
Estructura de Computadores, Paralelismo y Sistemas Distribuidos

Script final para ejecución en clúster MPI distribuido.
Soporta: rank/size, --dificultad, --max, --check-interval,
         parada temprana (isend/Iprobe), salida detallada.

Ejecución local:
    mpirun -n 4 python3 minero.py --dificultad 4 --max 5000000

Ejecución distribuida:
    mpirun -hostfile hosts -np 6 python3 /mnt/nfs/minero.py --dificultad 5 --max 50000000
"""

import hashlib
import time
import argparse
from mpi4py import MPI

# ──────────────────────────────────────────────
# Bloque de datos oficial (OBLIGATORIO / NO MODIFICABLE)
# ──────────────────────────────────────────────
BLOCK_ID  = 1
PREV_HASH = "0" * 64
DATA      = "Proyecto Cluster MPI EUNEIZ"

# Tags MPI
TAG_RESULT = 1   # Worker → Master: resultado exitoso
TAG_DONE   = 2   # Worker → Master: terminó sin éxito
TAG_STOP   = 99  # Master → Worker: señal de parada (rank ganador o -1)


def construir_cabecera(nonce: int) -> str:
    return f"{BLOCK_ID}|{PREV_HASH}|{DATA}|{nonce}"


def calcular_hash(cabecera: str) -> str:
    return hashlib.sha256(cabecera.encode('utf-8')).hexdigest()


def proceso_master(comm, rank: int, size: int, inicio: float) -> None:
    winner = -1
    done_count = 0

    while True:
        status = MPI.Status()
        msg = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        tag = status.Get_tag()
        source = status.Get_source()

        if tag == TAG_RESULT:
            winner = source
            print(f"\n[Master | rank={rank}] Solucion recibida del proceso {source}!")
            print(f"[Master | rank={rank}] Enviando señal de parada a {size-1} workers...")
            break
        elif tag == TAG_DONE:
            done_count += 1
            if done_count == size - 1:
                print(f"\n[Master | rank={rank}] Todos los workers agotaron el espacio. No se encontro nonce.")
                break

    # Enviar señal de parada a todos los workers
    for w in range(1, size):
        comm.send(winner, dest=w, tag=TAG_STOP)

    # Limpiar mensajes residuales
    while comm.Iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG):
        comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG)

    # Broadcast de validación desde el ganador
    if winner != -1:
        resultado_final = comm.bcast(None, root=winner)
        fin = time.time()
        elapsed = fin - inicio
        nonce_encontrado = resultado_final['nonce']
        hash_encontrado  = resultado_final['hash']
        intentos_total   = resultado_final['intentos_total']
        hashrate_global  = intentos_total / elapsed if elapsed > 0 else float('inf')

        print()
        print("=" * 65)
        print("  RESULTADO FINAL — MINERO DISTRIBUIDO SHA-256")
        print("=" * 65)
        print(f"  Nodo ganador    : rank {winner}")
        print(f"  Nonce           : {nonce_encontrado:,}")
        print(f"  Hash            : {hash_encontrado}")
        print(f"  Intentos totales: {intentos_total:,}")
        print(f"  Tiempo total    : {elapsed:.4f} s")
        print(f"  Hashrate global : {hashrate_global:,.2f} H/s")
        print(f"  Procesos MPI    : {size} (1 master + {size-1} workers)")
        print("=" * 65)
    else:
        fin = time.time()
        print(f"\n[Master] Tiempo total: {fin - inicio:.4f} s. Sin solucion en el rango dado.")


def proceso_worker(comm, rank: int, size: int, dificultad: int,
                   nonce_max: int, check_interval: int, inicio: float) -> None:
    objetivo = "0" * dificultad
    intentos = 0
    winner = -1
    resultado_local = None

    for nonce in range(rank, nonce_max + 1, size):
        intentos += 1

        # Parada temprana: Iprobe no bloqueante cada check_interval iteraciones
        if intentos % check_interval == 0:
            if comm.Iprobe(source=0, tag=TAG_STOP):
                winner = comm.recv(source=0, tag=TAG_STOP)
                print(f"[Worker {rank}] Señal de parada recibida (Iprobe). Abortando.")
                break

        cabecera = construir_cabecera(nonce)
        hash_hex = calcular_hash(cabecera)

        if hash_hex.startswith(objetivo):
            elapsed_local = time.time() - inicio
            hashrate_local = intentos / elapsed_local if elapsed_local > 0 else float('inf')
            print(f"\n[Worker {rank}] ¡NONCE ENCONTRADO! nonce={nonce:,} intentos={intentos:,} H/s={hashrate_local:,.0f}")

            resultado_local = {
                "nonce":         nonce,
                "cabecera":      cabecera,
                "hash":          hash_hex,
                "intentos":      intentos,
                "intentos_total": intentos * size,  # estimación global
                "rank":          rank,
            }
            comm.send(resultado_local, dest=0, tag=TAG_RESULT)

            # Esperar confirmación de parada del master
            winner = comm.recv(source=0, tag=TAG_STOP)
            break
    else:
        # Bucle agotado sin éxito
        comm.send(None, dest=0, tag=TAG_DONE)
        winner = comm.recv(source=0, tag=TAG_STOP)
        elapsed_local = time.time() - inicio
        print(f"[Worker {rank}] Espacio agotado. Intentos: {intentos:,} | Tiempo: {elapsed_local:.4f} s")

    # Validación broadcast desde el ganador
    if winner != -1:
        resultado_final = comm.bcast(
            resultado_local if rank == winner else None,
            root=winner
        )
        if rank != winner:
            print(f"[Worker {rank}] Bcast validado. Hash ganador: {resultado_final['hash'][:16]}...")


def minar_distribuido(dificultad: int, nonce_max: int, check_interval: int) -> None:
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if size < 2:
        if rank == 0:
            print("ERROR: Se necesitan al menos 2 procesos (1 master + N workers).")
            print("Uso: mpirun -n <N>=2 python3 minero.py [--dificultad D] [--max M]")
        return

    if rank == 0:
        objetivo = "0" * dificultad
        print("=" * 65)
        print("       MINERO DISTRIBUIDO SHA-256 — AE3 HITO FINAL")
        print("=" * 65)
        print(f"  Bloque ID       : {BLOCK_ID}")
        print(f"  Hash previo     : {PREV_HASH[:20]}...")
        print(f"  Datos           : {DATA}")
        print(f"  Dificultad      : {dificultad} ceros (prefijo '{objetivo}')")
        print(f"  Nonce maximo    : {nonce_max:,}")
        print(f"  Check interval  : cada {check_interval:,} iteraciones")
        print(f"  Procesos MPI    : {size} (1 master + {size-1} workers)")
        print("=" * 65)

    comm.Barrier()
    inicio = time.time()

    if rank == 0:
        proceso_master(comm, rank, size, inicio)
    else:
        proceso_worker(comm, rank, size, dificultad, nonce_max, check_interval, inicio)


def main():
    parser = argparse.ArgumentParser(
        description="Minero distribuido SHA-256 — AE3 Hito Final"
    )
    parser.add_argument("--dificultad",      type=int, default=4,
                        help="Ceros hexadecimales al inicio del hash (default: 4)")
    parser.add_argument("--max",             type=int, default=5_000_000,
                        help="Nonce maximo a probar (default: 5000000)")
    parser.add_argument("--check-interval",  type=int, default=10_000,
                        help="Frecuencia de Iprobe para parada temprana (default: 10000)")
    args = parser.parse_args()

    if args.dificultad < 1:
        parser.error("La dificultad debe ser >= 1.")
    if args.max < 1:
        parser.error("El maximo debe ser >= 1.")
    if args.check_interval < 1:
        parser.error("El check-interval debe ser >= 1.")

    minar_distribuido(args.dificultad, args.max, args.check_interval)


if __name__ == "__main__":
    main()
