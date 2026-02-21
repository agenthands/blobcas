package core

// CID represents binary CID bytes.
type CID struct {
	Bytes []byte
}

// Ref references a stored object by its manifest CID.
type Ref struct {
	ManifestCID CID
}

// Key represents a logical key for a stored object.
type Key struct {
	Namespace string
	ID        string
}
