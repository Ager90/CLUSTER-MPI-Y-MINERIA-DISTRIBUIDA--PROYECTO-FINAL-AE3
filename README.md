# Clúster MPI y Minería SHA-256 Distribuida -- PROYECTO FINAL AE3

Este repositorio contiene el proyecto final desarrollado para la asignatura de Estructura de Computadores, Paralelismo y Sistemas Distribuidos. El objetivo del proyecto es construir y optimizar un clúster MPI para minería distribuida.


***

## Fases del Proyecto

El desarrollo del clúster se divide en hitos, siendo cada uno una tarea entregable:

*   **Hito 1: Minero Secuencial:** Implementación base en Python de un minero de Prueba de Trabajo (Proof of Work) usando SHA-256 en un solo núcleo, estableciendo las métricas iniciales de rendimiento (H/s).
  
*   **Hito 2: Perfilado de Código (Profiling):** Análisis del rendimiento interno con `cProfile` y monitorización del sistema con `htop` para identificar los cuellos de botella y justificar la necesidad de escalar a múltiples núcleos.
  
*   **Hito 3: Paralelización Estática (MPI):** Integración de OpenMPI y `mpi4py` para lanzar procesos paralelos. El espacio de búsqueda del *nonce* se divide matemáticamente entre los núcleos disponibles para exprimir el 100% de la CPU local.
  
*   **Hito 4: Sincronización y "Parada Temprana":** Implementación de comunicación no bloqueante (`Iprobe` y `bcast`). Cuando un proceso encuentra la solución, avisa al resto del clúster para detener la ejecución inmediatamente y no desperdiciar recursos.
  
*   **Hito 5: Expansión de la Red y Segundo Nodo:** Transición desde una ejecución local a una infraestructura distribuida real mediante la clonación de la máquina virtual original para crear un segundo nodo (Worker). Esta fase incluye la creación de una red privada en VirtualBox, la asignación de direcciones IP estáticas mediante Netplan, y la configuración de la resolución por nombre en `/etc/hosts` para garantizar una comunicación estable entre las máquinas.


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
---
