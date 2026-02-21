package core

import (
	"time"
)

type Config struct {
	Dir string // repo root

	Chunking  ChunkingConfig
	Pack      PackConfig
	Catalog   CatalogConfig
	Limits    LimitsConfig
	Transform TransformConfig
	GC        GCConfig
}

type ChunkingConfig struct {
	DefaultProfile string
	Min            int
	Avg            int
	Max            int
	Normalization  int
}

type PackConfig struct {
	Dir                string
	TargetPackBytes    uint64
	MaxPackBytes       uint64
	RequireIndexOnSeal bool
	SealFsync          bool
	FsyncEveryBytes    uint64
}

type CatalogConfig struct {
	Dir     string
	Backend string
}

type TransformConfig struct {
	Name      string
	ZstdLevel int
	KeyID     string
	KeyBytes  []byte
}

type LimitsConfig struct {
	MaxCanonicalBytes  uint64
	MaxRawWireBytes    uint64
	MaxChunksPerObject uint32
	MaxTags            int
	MaxTagKeyLen       int
	MaxTagValLen       int
	MaxMediaTypeLen    int
}

type GCConfig struct {
	Enabled         bool
	DefaultRootTTL  time.Duration
	RunEvery        time.Duration
	MaxPackAge      time.Duration
	TargetPackBytes uint64
	MaxIOPS         int
}
