#!/usr/bin/env python3
"""
Minero secuencial SHA-256 — Hito 1
Estructura de Computadores, Paralelismo y Sistemas Distribuidos

Implementa un minero secuencial que busca un nonce cuyo hash SHA-256
cumpla una condición de dificultad (N ceros hexadecimales al inicio).
"""

import hashlib
import time
import argparse


# ──────────────────────────────────────────────
# Bloque de datos 
# ──────────────────────────────────────────────
BLOCK_ID = 1
PREV_HASH = "0" * 64
DATA = "Proyecto Cluster MPI EUNEIZ"


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


def minar(dificultad: int, nonce_max: int) -> None:
    """
    Ejecuta el minero secuencial:
       Itera nonce desde 0 hasta nonce_max.
       Comprueba si el hash comienza con 'dificultad' ceros hexadecimales.
       Mide tiempo total, intentos y rendimiento (H/s).
    """
    objetivo = "0" * dificultad  # prefijo que debe cumplir el hash

    print("=" * 60)
    print("        MINERO SECUENCIAL SHA-256 — HITO 1")
    print("=" * 60)
    print(f"  Bloque ID   : {BLOCK_ID}")
    print(f"  Hash previo  : {PREV_HASH}")
    print(f"  Datos        : {DATA}")
    print(f"  Dificultad   : {dificultad} (prefijo '{objetivo}')")
    print(f"  Nonce máximo : {nonce_max:,}")
    print("=" * 60)
    print()

    inicio = time.time()

    for nonce in range(nonce_max + 1):
        cabecera = construir_cabecera(nonce)
        hash_hex = calcular_hash(cabecera)

        if hash_hex.startswith(objetivo):
            fin = time.time()
            tiempo_total = fin - inicio
            intentos = nonce + 1
            hashrate = intentos / tiempo_total if tiempo_total > 0 else float('inf')

            print("¡NONCE ENCONTRADO!")
            print("-" * 60)
            print(f"  Nonce        : {nonce}")
            print(f"  Cabecera     : {cabecera}")
            print(f"  Hash         : {hash_hex}")
            print(f"  Intentos     : {intentos:,}")
            print(f"  Tiempo total : {tiempo_total:.4f} s")
            print(f"  Rendimiento  : {hashrate:,.2f} H/s")
            print("-" * 60)
            return

    # Si no se encontró un nonce válido en el rango
    fin = time.time()
    tiempo_total = fin - inicio
    hashrate = (nonce_max + 1) / tiempo_total if tiempo_total > 0 else float('inf')

    print("NO se encontró un nonce válido en el rango especificado.")
    print("-" * 60)
    print(f"  Intentos     : {nonce_max + 1:,}")
    print(f"  Tiempo total : {tiempo_total:.4f} s")
    print(f"  Rendimiento  : {hashrate:,.2f} H/s")
    print("-" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Minero secuencial SHA-256 — Hito 1"
    )
    parser.add_argument(
        "--dificultad",
        type=int,
        required=True,
        help="Número de ceros hexadecimales al inicio del hash."
    )
    parser.add_argument(
        "--max",
        type=int,
        default=100_000_000,
        help="Nonce máximo a probar (por defecto 100 000 000)."
    )

    args = parser.parse_args()

    if args.dificultad < 1:
        parser.error("La dificultad debe ser >= 1.")

    minar(args.dificultad, args.max)


if __name__ == "__main__":
    main()
