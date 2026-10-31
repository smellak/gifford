package dataset

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"

	"github.com/smellak/gifford/internal/protocol"
)

// splitDomain separates split-v1's hash from every other use of the
// group digest.
const splitDomain = "gifford.dataset.split.v1"

// SplitV1 assigns a group's split from the group digest alone: the
// SHA-256 of the domain and the digest gives a number from 0 to 99; train
// below 70, validation from 70 to 84, test from 85. The same group always
// lands in the same split, in any run.
type SplitV1 struct{}

// Split implements Splitter.
func (SplitV1) Split(group protocol.Digest) (Split, error) {
	if !group.Valid() {
		return "", errors.New("split: not a group digest")
	}
	h := sha256.New()
	_, _ = io.WriteString(h, splitDomain)
	_, _ = h.Write([]byte{0})
	_, _ = io.WriteString(h, string(group))
	n := binary.BigEndian.Uint64(h.Sum(nil)[:8]) % 100
	switch {
	case n < 70:
		return SplitTrain, nil
	case n < 85:
		return SplitValidation, nil
	default:
		return SplitTest, nil
	}
}

// SplitTable assigns each group the split the table gives it, as a
// split file assigns sessions. A group the table does not list has no
// split, and its window fails the run: none falls into a split by
// default.
type SplitTable map[protocol.Digest]Split

// Split implements Splitter.
func (t SplitTable) Split(group protocol.Digest) (Split, error) {
	s, ok := t[group]
	if !ok {
		return "", errors.New("split: group not in the table")
	}
	return s, nil
}
