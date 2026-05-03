package domain

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

const TotalShards = 3

type ShardID = string
type CellRole = string

const (
	RoleActive  CellRole = "active"
	RolePassive CellRole = "passive"
)

// Route retorna "shard-1", "shard-2" ou "shard-3"
func Route(routingKey string) ShardID {
	h := sha256.Sum256([]byte(routingKey))
	n := binary.BigEndian.Uint64(h[:8])
	return fmt.Sprintf("shard-%d", (n%TotalShards)+1)
}
