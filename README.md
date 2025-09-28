# CacheRing

**CacheRing — A lightweight, sharded, distributed cache in Go for learning purposes.**

## Features

- [ ] Sharded in-node store
- [ ] Data types: `string`, `hash`, `sorted set`
- [ ] Cluster routing via consistent-hashing ring
- [ ] Replication (configurable)
- [ ] gRPC API + optional HTTP gateway
- [ ] Prometheus metrics


## Project Layout(Minimal Version)

```
CacheRing/
├─ cmd/                    
│  ├─ cache-node/          
│  └─ cachectl/            
├─ api/                    
│  ├─ proto/               
│  └─ gateway/             
├─ internal/               
│  ├─ store/               
│  ├─ cluster/             
│  ├─ membership/          
│  ├─ transport/           
│  └─ metrics/             
├─ pkg/                    
├─ deployments/            
├─ scripts/                
├─ test/                                   
├─ configs/                
├─ Makefile
├─ go.mod
└─ README.md
```

