| Key | Default | Type | Description |
|-----|---------|------|-------------|
| high-availability.kubernetes.leader-election.lease-duration | 15 s | Duration | Define the lease duration for the Kubernetes leader election. The leader will continuously renew its lease time to indicate its existence. And the followers will do a lease checking against the current time. "renewTime + leaseDuration > now" means the leader is alive. |
| high-availability.kubernetes.leader-election.renew-deadline | 15 s | Duration | Defines the deadline duration when the leader tries to renew the lease. The leader will give up its leadership if it cannot successfully renew the lease in the given time. |
| high-availability.kubernetes.leader-election.retry-period | 5 s | Duration | Defines the pause duration between consecutive retries. All the contenders, including the current leader and all other followers, periodically try to acquire/renew the leadership if possible at this interval. |
