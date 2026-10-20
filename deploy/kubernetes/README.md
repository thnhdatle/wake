# Wake on Kubernetes

Plain manifests for a single-replica StatefulSet (one replication slot consumer
with persistent state). Details: [operations guide](../../docs/guides/operations.md#kubernetes).

| File | Content |
|---|---|
| `statefulset.yaml` | StatefulSet, 1 replica, PVC `data` (1Gi) for `state.path` and `stream.spill_dir` |
| `configmap.yaml` | Wake configuration; edit source, tables and sinks |
| `service.yaml` | Headless Service and `wake-admin` Service (port 9093, Prometheus annotations) |
| `secret.example.yaml` | Example Secret `wake-source` with the source DSN (placeholder; not applied by kustomize) |
| `kustomization.yaml` | Lists the manifests above, without the Secret |

```sh
kubectl create namespace wake
kubectl -n wake create secret generic wake-source \
  --from-literal=dsn='postgres://wake:<password>@<host>:5432/<db>'
kubectl -n wake apply -k deploy/kubernetes
kubectl -n wake rollout status statefulset/wake
```

Size the PVC for the largest transaction you expect: transactions above
`stream.max_tx_bytes` spill to `/var/lib/wake/spill`. Restrict port 9093 with a
NetworkPolicy; the admin endpoints are not authenticated. Validate offline with
`kubeconform -strict -summary deploy/kubernetes`.
