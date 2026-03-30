import hashlib
import time
import argparse

from mpi4py import MPI

#!/usr/bin/env python3
"""
Minero MPI básico SHA-256 — Hito 3
Estructura de Computadores, Paralelismo y Sistemas Distribuidos

Paraleliza la búsqueda del nonce mediante MPI (mpi4py).
Cada proceso recibe un subconjunto disjunto del espacio de nonces:
    nonce = rank + i * size   →   range(rank, max_nonce, size)

Ejecución:
    mpirun -n <N> python3 minero_mpi_basico.py --dificultad 4 --max 2000000
"""

# ──────────────────────────────────────────────
# Bloque de datos oficial (OBLIGATORIO / NO MODIFICABLE)
# ──────────────────────────────────────────────
BLOCK_ID  = 1
PREV_HASH = "0" * 64
DATA      = "Proyecto Cluster MPI EUNEIZ"


def construir_cabecera(nonce: int) -> str:
    """
    Construye la cabecera del bloque con el formato oficial:
        block_id|prev_hash|data|nonce
    """
    return f"{BLOCK_ID}|{PREV_HASH}|{DATA}|{nonce}"


def calcular_hash(cabecera: str) -> str:
    """
    Calcula el hash SHA-256 de la cabecera codificada en UTF-8.
    Devuelve el hash en formato hexadecimal (64 caracteres).
    """
    return hashlib.sha256(cabecera.encode('utf-8')).hexdigest()


def minar_mpi(dificultad: int, nonce_max: int) -> None:
    """
    Ejecuta el minero paralelo con MPI:
      - Cada proceso prueba únicamente los nonces de su rango:
            range(rank, nonce_max + 1, size)
      - El proceso que encuentra el nonce (rank 0 o cualquier otro)
        imprime el resultado completo.
      - Los demás procesos informan de su contribución a los intentos.
    """
    # ── Inicialización MPI ───────────────────────────────────────────
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()   # Identificador de este proceso (0 … size-1)
    size = comm.Get_size()   # Número total de procesos

    objetivo = "0" * dificultad

    # Solo el proceso 0 (Master) imprime la cabecera informativa
    if rank == 0:
        print("=" * 60)
        print("      MINERO MPI BÁSICO SHA-256 — HITO 3")
        print("=" * 60)
        print(f"  Bloque ID    : {BLOCK_ID}")
        print(f"  Hash previo  : {PREV_HASH}")
        print(f"  Datos        : {DATA}")
        print(f"  Dificultad   : {dificultad} (prefijo '{objetivo}')")
        print(f"  Nonce máximo : {nonce_max:,}")
        print(f"  Procesos MPI : {size}")
        print("=" * 60)
        print()

    # Pequeña barrera para que todos los procesos empiecen a la vez
    comm.Barrier()
    inicio = time.time()

    # ── Búsqueda paralela con reparto estático ───────────────────────
    # Cada proceso prueba: rank, rank+size, rank+2*size, …
    for nonce in range(rank, nonce_max + 1, size):
        cabecera = construir_cabecera(nonce)
        hash_hex = calcular_hash(cabecera)

        if hash_hex.startswith(objetivo):
            fin          = time.time()
            tiempo_total = fin - inicio
            intentos     = (nonce - rank) // size + 1   # intentos de ESTE proceso
            hashrate     = intentos / tiempo_total if tiempo_total > 0 else float('inf')

            print(f"\n[Proceso {rank}] ¡NONCE ENCONTRADO!")
            print("-" * 60)
            print(f"  Nonce        : {nonce}")
            print(f"  Cabecera     : {cabecera}")
            print(f"  Hash         : {hash_hex}")
            print(f"  Intentos     : {intentos:,}  (solo este proceso)")
            print(f"  Tiempo total : {tiempo_total:.4f} s")
            print(f"  Rendimiento  : {hashrate:,.2f} H/s  (solo este proceso)")
            print("-" * 60)
            return  # Este proceso termina; los demás seguirán su bucle

    # ── Sin solución encontrada por este proceso ─────────────────────
    fin          = time.time()
    tiempo_total = fin - inicio
    intentos     = len(range(rank, nonce_max + 1, size))
    hashrate     = intentos / tiempo_total if tiempo_total > 0 else float('inf')

    print(f"[Proceso {rank}] No encontró solución en su rango.")
    print(f"  Intentos     : {intentos:,}")
    print(f"  Tiempo total : {tiempo_total:.4f} s")
    print(f"  Rendimiento  : {hashrate:,.2f} H/s")


def main():
    parser = argparse.ArgumentParser(
        description="Minero MPI básico SHA-256 — Hito 3"
    )
    parser.add_argument(
        "--dificultad",
        type=int,
        default=4,
        help="Número de ceros hexadecimales al inicio del hash."
    )
    parser.add_argument(
        "--max",
        type=int,
        default=2_000_000,
        help="Nonce máximo a probar (por defecto 2 000 000)."
    )

    args = parser.parse_args()

    if args.dificultad < 1:
        parser.error("La dificultad debe ser >= 1.")

    minar_mpi(args.dificultad, args.max)


if __name__ == "__main__":
    main()
