package memory

import (
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type Allocator = memory.Allocator

var DefaultAllocator Allocator = memory.NewGoAllocator()
