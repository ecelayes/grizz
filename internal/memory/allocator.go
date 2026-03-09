package memory

import (
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type Allocator interface {
	memory.Allocator
}

func DefaultAllocator() Allocator {
	return memory.NewGoAllocator()
}
