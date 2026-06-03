# Clúster MPI y Minería SHA-256 Distribuida -- PROYECTO FINAL AE3

Este repositorio contiene el proyecto final grupal AE3 desarrollado para la asignatura de Estructura de Computadores, Paralelismo y Sistemas Distribuidos. El objetivo del proyecto es construir y optimizar un clúster MPI para minería distribuida.


***

##  Hitos del Proyecto
 
El desarrollo se estructuró en 6 hitos acumulativos:
 
| Hito | Descripción | Tecnología clave | Resultado |
|------|-------------|-----------------|-----------|
| **Hito 1** | Minero secuencial en Python | `hashlib`, `argparse` | 1.28 MH/s, 1 núcleo al 100% |
| **Hito 2** | Perfilado de código | `cProfile` | 97% del tiempo en SHA-256 → p=0.97 |
| **Hito 3** | Paralelización estática MPI | `mpi4py`, `mpirun` | 3.90 MH/s con 2 procesos |
| **Hito 4** | Parada temprana no bloqueante | `Iprobe`, `isend`, `bcast` | Todos los procesos terminan limpiamente |
| **Hito 5** | Expansión a 2 nodos | VirtualBox, Netplan, `/etc/hosts` | Red interna funcional, ping 0% pérdida |
| **Hito Final** | Clúster completo (3 nodos) | NFS, SSH, hostfile MPI | 6.50 MH/s · ×3.04 speedup distribuido |
 
---


## ⚙️ Instalación y Ejecución
 
### Requisitos
 
```bash
sudo apt update && sudo apt install -y python3 python3-pip openmpi-bin libopenmpi-dev
pip3 install mpi4py
```
 
### 1. Ejecución secuencial
 
```bash
python3 src/minero_secuencial.py --dificultad 4 --max 5000000
```
 
### 2. MPI local (múltiples procesos en un nodo)
 
```bash
# Con parada temprana (recomendado)
mpirun -n 4 python3 src/minero_mpi_optimizado.py --dificultad 4 --max 5000000
```
 
### 3. MPI distribuido (clúster completo)
 
```bash
# Desde master, con NFS montado y SSH configurado
mpirun -hostfile src/hosts -np 6 python3 /mnt/nfs/minero.py \
    --dificultad 5 --max 50000000
```
---

 
## 🖧 Arquitectura del Clúster
 
| Nodo | Hostname | IP | Slots MPI | Rol | H/s |
|------|----------|----|-----------|-----|-----|
| 0 | `master` | 192.168.50.10 | 2 | Master + Worker | ~2.05 MH/s |
| 1 | `worker1` | 192.168.50.11 | 2 | Worker | ~1.25 MH/s |
| 2 | `worker2` | 192.168.50.12 | 2 | Worker | ~1.25 MH/s |
 
- **Red interna VirtualBox** (adaptador 2) + NAT para Internet (adaptador 1)
- **NFS** exportado desde master, montado en workers → directorio `/mnt/nfs` compartido
- **SSH sin contraseña** desde master → workers (necesario para que `mpirun` lance procesos remotos)
- **Resolución por nombre** via `/etc/hosts` en los 3 nodos

