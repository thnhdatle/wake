# Wake on Kubernetes

Deploy Wake as a single-replica StatefulSet. Details: [operations guide](../../docs/guides/operations.md#kubernetes).

```sh
kubectl create namespace wake
kubectl -n wake create secret generic wake-source \
  --from-literal=dsn='postgres://wake:<password>@<host>:5432/<db>'   # source DSN
kubectl -n wake apply -k deploy/kubernetes                          # everything but the Secret
kubectl -n wake rollout status statefulset/wake
kubeconform -strict -summary deploy/kubernetes                      # validate offline
```

| File | Content |
|---|---|
| `statefulset.yaml` | 1 replica, PVC `data` (1Gi) for state and spill |
| `configmap.yaml` | Wake config; edit source, tables, sinks |
| `service.yaml` | headless Service and `wake-admin` (port 9093) |
| `secret.example.yaml` | example Secret `wake-source`; not applied |
| `kustomization.yaml` | the manifests above, no Secret |

Size the PVC for your largest transaction: anything above `stream.max_tx_bytes` spills to
`/var/lib/wake/spill`.

**Restrict port 9093 with a NetworkPolicy.** The admin endpoints are not authenticated.
