package bench

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/sagar0x0/stratum"
)

type Workload struct {
	ReadPercent   int
	UpdatePercent int
	RMWPercent    int
}

var (
	WorkloadA = Workload{ReadPercent: 50, UpdatePercent: 50}
	WorkloadB = Workload{ReadPercent: 95, UpdatePercent: 5}
	WorkloadC = Workload{ReadPercent: 100}
	WorkloadF = Workload{ReadPercent: 50, RMWPercent: 50}
)

type YCSB struct {
	db          *stratum.DB
	numKeys     int
	keySize     int
	valSize     int
	zipf        *rand.Zipf
	rnd         *rand.Rand
	bufPool     sync.Pool
	recordCount int
}

func NewYCSB(db *stratum.DB, numKeys, keySize, valSize int) *YCSB {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	zipf := rand.NewZipf(r, 1.1, 1, uint64(numKeys-1))

	return &YCSB{
		db:      db,
		numKeys: numKeys,
		keySize: keySize,
		valSize: valSize,
		zipf:    zipf,
		rnd:     r,
		bufPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, valSize)
			},
		},
	}
}

func (y *YCSB) Load() error {
	for i := 0; i < y.numKeys; i++ {
		key := []byte(fmt.Sprintf("user%0*d", y.keySize-4, i))
		val := y.bufPool.Get().([]byte)
		y.rnd.Read(val)
		
		if err := y.db.Put(key, val); err != nil {
			return err
		}
		y.bufPool.Put(val)
	}
	y.recordCount = y.numKeys
	return nil
}

func (y *YCSB) Run(workload Workload, ops int) error {
	for i := 0; i < ops; i++ {
		opType := y.rnd.Intn(100)
		
		keyIdx := y.zipf.Uint64()
		key := []byte(fmt.Sprintf("user%0*d", y.keySize-4, keyIdx))

		if opType < workload.ReadPercent {
			_, err := y.db.Get(key)
			if err != nil && err != stratum.ErrNotFound {
				return err
			}
		} else if opType < workload.ReadPercent+workload.UpdatePercent {
			val := y.bufPool.Get().([]byte)
			y.rnd.Read(val)
			err := y.db.Put(key, val)
			y.bufPool.Put(val)
			if err != nil {
				return err
			}
		} else if workload.RMWPercent > 0 && opType < workload.ReadPercent+workload.UpdatePercent+workload.RMWPercent {
			val, err := y.db.Get(key)
			if err != nil && err != stratum.ErrNotFound {
				return err
			}
			
			newVal := y.bufPool.Get().([]byte)
			if val != nil {
				copy(newVal, val)
				newVal[0] = ^newVal[0] 
			} else {
				y.rnd.Read(newVal)
			}
			
			err = y.db.Put(key, newVal)
			y.bufPool.Put(newVal)
			if err != nil {
				return err
			}
		} else {
			newKeyIdx := y.recordCount
			y.recordCount++
			insertKey := []byte(fmt.Sprintf("user%0*d", y.keySize-4, newKeyIdx))
			val := y.bufPool.Get().([]byte)
			y.rnd.Read(val)
			err := y.db.Put(insertKey, val)
			y.bufPool.Put(val)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
