# CLUSTER MPI Y MINERÍA DISTRIBUIDA -- PROYECTO FINAL AE3

Este repositorio contiene el proyecto final desarrollado para la asignatura de Estructura de Computadores, Paralelismo y Sistemas Distribuidos. El objetivo del proyecto es construir y optimizar un clúster MPI para minería distribuida.


***

## Fases del Proyecto

El desarrollo del clúster se divide en hitos, siendo cada uno una tarea entregable:

*   **Hito 1: Minero Secuencial:** Implementación base en Python de un minero de Prueba de Trabajo (Proof of Work) usando SHA-256 en un solo núcleo, estableciendo las métricas iniciales de rendimiento (H/s).
  
*   **Hito 2: Perfilado de Código (Profiling):** Análisis del rendimiento interno con `cProfile` y monitorización del sistema con `htop` para identificar los cuellos de botella y justificar la necesidad de escalar a múltiples núcleos.
  
*   **Hito 3: Paralelización Estática (MPI):** Integración de OpenMPI y `mpi4py` para lanzar procesos paralelos. El espacio de búsqueda del *nonce* se divide matemáticamente entre los núcleos disponibles para exprimir el 100% de la CPU local.
  
*   **Hito 4: Sincronización y "Parada Temprana":** Implementación de comunicación no bloqueante (`Iprobe` y `bcast`). Cuando un proceso encuentra la solución, avisa al resto del clúster para detener la ejecución inmediatamente y no desperdiciar recursos.
  
*   **Hito 5: Expansión de la Red y Segundo Nodo:** Transición desde una ejecución local a una infraestructura distribuida real mediante la clonación de la máquina virtual original para crear un segundo nodo (Worker). Esta fase incluye la creación de una red privada en VirtualBox, la asignación de direcciones IP estáticas mediante Netplan, y la configuración de la resolución por nombre en `/etc/hosts` para garantizar una comunicación estable entre las máquinas.
