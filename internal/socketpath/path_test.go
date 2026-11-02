package socketpath

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckBoundary(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		length  int
		wantErr bool
	}{
		{name: "limit fits", length: MaxBytes},
		{name: "one byte over", length: MaxBytes + 1, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			path := strings.Repeat("x", test.length)
			err := Check(path)
			if !test.wantErr {
				require.NoError(t, err)
				return
			}
			var lengthErr *LengthError
			require.ErrorAs(t, err, &lengthErr)
			require.Equal(t, path, lengthErr.Path)
			require.Equal(t, test.length, lengthErr.Length)
			require.Equal(t, MaxBytes, lengthErr.Limit)
			require.EqualError(t, err,
				"socket path "+path+" is "+strconv.Itoa(test.length)+
					" bytes, the limit is "+strconv.Itoa(MaxBytes)+
					"; use a shorter XDG_RUNTIME_DIR")
		})
	}
}
