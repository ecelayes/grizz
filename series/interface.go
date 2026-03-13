package series

import (
	grizzarrows "github.com/ecelayes/grizz/internal/arrow"
)

type Series interface {
	Name() string
	SetName(name string)
	Type() grizzarrows.DataType
	Len() int
	IsNull(i int) bool
	Release()
}
