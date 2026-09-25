package model

import "distributed-kv-datastore/internal/vectorclock"

type DataItem struct {
	Value         any
	VectorClock   *vectorclock.VectorClock
	LastUpdatedBy string
	IsDeleted     bool
}
