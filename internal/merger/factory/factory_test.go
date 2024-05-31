package factory

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test(t *testing.T) {

	q := 1 | 2 | 4

	require.Equal(t, q, 0|q|q|q|q|q)
	t.Log(0 | 1 | 1 | 1)
	t.Log(0 | 2)
	t.Log(0 | 4)
	t.Log(0 | 3)
}
